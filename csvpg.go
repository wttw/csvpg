package csvpg

//go:generate go run genkeywords/main.go

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/araddon/dateparse"
)

// RowReader reads a single entry from, e.g., a CSV file
type RowReader interface {
	Read() ([]string, error)
}

// Config for csvgpg
type Config struct {
	SnakeCase   bool
	Sample      int
	ColumnNames []string
	NotNull     bool
	ReadHeader  bool
	EnumLength  int
	EnumCount   int
	Types       []Type
	Exclude     []string
	TableName   string
}

// Type holds the config info about each database type
type Type struct {
	Name    string
	Pattern string
	Isa     func(string) bool
	Handle  func(string, int, *Intuitor) bool
	flag    int
}

var enumRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// NewConfig generates a default configuration
func NewConfig() Config {
	return Config{
		SnakeCase:  true,
		ReadHeader: true,
		EnumLength: 10,
		EnumCount:  20,
		Types: []Type{
			Type{
				Name: "integer",
				Isa: func(s string) bool {
					_, err := strconv.ParseInt(s, 10, 32)
					return err == nil
				},
			},
			Type{
				Name: "bigint",
				Isa: func(s string) bool {
					_, err := strconv.ParseInt(s, 10, 64)
					return err == nil
				},
			},
			Type{
				Name: "real",
				Isa: func(s string) bool {
					_, err := strconv.ParseFloat(s, 32)
					return err == nil
				},
			},
			Type{
				Name: "double",
				Isa: func(s string) bool {
					_, err := strconv.ParseFloat(s, 64)
					return err == nil
				},
			},
			Type{
				Name:    "numeric",
				Pattern: `^[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`,
			},
			Type{
				Name:    "boolean",
				Pattern: `^(?i:t|tr|tru|true|y|ye|yes|on|1|f|fa|fal|fals|false|n|no|of|off|0)$`,
			},
			Type{
				Name:    "bytea",
				Pattern: `^\\x(?i:[0-9a-f][0-9a-f])*$`,
			},
			Type{
				Name:    "uuid",
				Pattern: `^(?i:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\})$`,
			},
			Type{
				Name: "cidr",
				Isa: func(s string) bool {
					addr, net, err := net.ParseCIDR(s)
					if err != nil {
						return false
					}
					if bytes.Compare(addr, net.IP) == 0 {
						// address == network implies no non-zero bits right of the netmask
						return true
					}
					return false
				},
			},
			Type{
				Name: "inet",
				Isa: func(s string) bool {
					if net.ParseIP(s) != nil {
						// IPv4 or v6 literal
						return true
					}
					_, _, err := net.ParseCIDR(s)
					// CIDR type with non-zero bits right of the netmask
					return err == nil
				},
			},
			Type{
				Name:    "macaddr",
				Pattern: `^(?i:(?:[0-9a-f]{2}:){5}[0-9a-f]{2}|(?:[0-9a-f]{2}-){5}[0-9a-f]{2})$`,
			},
			Type{
				Name: "date",
				Isa: func(s string) bool {
					t, err := dateparse.ParseAny(s)
					if err != nil {
						return false
					}
					return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0
				},
			},
			Type{
				Name: "timestamptz",
				Isa: func(s string) bool {
					_, err := dateparse.ParseAny(s)
					return err == nil
				},
			},
			Type{
				Name: "enum",
				Handle: func(s string, col int, it *Intuitor) bool {
					if len(s) == 0 || len(s) > it.Config.EnumLength {
						return false
					}
					if !enumRe.MatchString(s) {
						return false
					}
					_, ok := it.Enums[col][s]
					if ok {
						return true
					}
					it.Enums[col][s] = struct{}{}
					return len(it.Enums[col]) <= it.Config.EnumCount
				},
			},
			Type{
				Name: "text",
				Isa: func(string) bool {
					return true
				},
			},
		},
	}
}

func (c *Config) tidy() error {
	flag := 1
	for i, v := range c.Types {
		c.Types[i].flag = flag
		flag = flag << 1

		if v.Handle != nil {
			continue
		}

		if v.Isa == nil {
			if v.Pattern == "" {
				return fmt.Errorf("No logic provided for type '%s'", v.Name)
			}
			re, err := regexp.Compile(v.Pattern)
			if err != nil {
				return fmt.Errorf("Invalid pattern for type '%s': %s", v.Name, err.Error())
			}
			c.Types[i].Handle = makeMatcher(re)
			continue
		}

		c.Types[i].Handle = makeWrapper(v.Isa)
	}

	if c.TableName == "" {
		c.TableName = "my_table"
	}
	return nil
}

func makeMatcher(re *regexp.Regexp) func(s string, col int, it *Intuitor) bool {
	return func(s string, _ int, _ *Intuitor) bool {
		return re.MatchString(s)
	}
}

func makeWrapper(f func(s string) bool) func(s string, col int, it *Intuitor) bool {
	return func(s string, _ int, _ *Intuitor) bool {
		return f(s)
	}
}

// Intuitor intuits the types of the input
type Intuitor struct {
	Config        *Config
	ColumnNames   []string
	PossibleTypes []int
	Enums         []map[string]struct{}
	ColumnTypes   []string
	Null          []bool
	row           int
	EnumDDL       string
	TableDDL      string
}

// NewIntuitor creates a new Intuitor
func NewIntuitor(c *Config) *Intuitor {
	return &Intuitor{
		Config: c,
	}
}

// Intuit does the work of identifying column types
func (c *Intuitor) Intuit(reader RowReader) error {
	err := c.Config.tidy()
	if err != nil {
		return err
	}
	// Read the CSV header, if we have one
	c.row = 0
	if c.Config.ReadHeader {
		header, err := reader.Read()
		c.row++
		if err != nil {
			return fmt.Errorf("Failed to read header: %s", err.Error())
		}
		c.ColumnNames = make([]string, len(header))
		for i, v := range header {
			if c.Config.SnakeCase {
				c.ColumnNames[i] = snake(v)
			} else {
				c.ColumnNames[i] = v
			}
		}
	}

	// Read the first row, so we can get the number of columns
	firstrow, err := reader.Read()
	c.row++
	if err != nil {
		return fmt.Errorf("Failed to read first row: %s", err.Error())
	}

	if len(c.ColumnNames) == 0 {
		if len(c.Config.ColumnNames) != 0 {
			c.ColumnNames = c.Config.ColumnNames
		} else {
			c.ColumnNames = make([]string, len(firstrow))
			for i := range firstrow {
				c.ColumnNames[i] = fmt.Sprintf("col%d", i)
			}
		}
	}

	if len(firstrow) != len(c.ColumnNames) {
		return fmt.Errorf("Header length doesn't match length of first row, %d vs %d", len(c.ColumnNames), len(firstrow))
	}

	// Initialize our state for the scan
	allPossibleTypes := 0

TYPE:
	for _, v := range c.Config.Types {
		if v.Name == "enum" && c.Config.EnumCount == 0 {
			continue
		}

		for _, exclude := range c.Config.Exclude {
			if exclude == v.Name {
				continue TYPE
			}
		}

		allPossibleTypes = allPossibleTypes | v.flag
	}

	c.PossibleTypes = make([]int, len(c.ColumnNames))
	for i := range c.PossibleTypes {
		c.PossibleTypes[i] = allPossibleTypes
	}

	c.Enums = make([]map[string]struct{}, len(c.ColumnNames))
	for i := range c.Enums {
		c.Enums[i] = map[string]struct{}{}
	}

	c.Null = make([]bool, len(c.ColumnNames))

	// Handle our previously read first row
	err = c.handleRow(firstrow)
	if err != nil {
		return err
	}

	// Handle the rest of the file
	for {
		if c.Config.Sample != 0 && c.row > c.Config.Sample {
			break
		}
		row, err := reader.Read()
		c.row++
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Error reading row %d: %s", c.row, err.Error())
		}
		if len(row) != len(c.ColumnNames) {
			return fmt.Errorf("Row %d has the wrong number of fields, %d vs %d", c.row, len(row), len(c.ColumnNames))
		}
		err = c.handleRow(row)
		if err != nil {
			return err
		}
	}

	// Decide on column types
	c.ColumnTypes = make([]string, len(c.ColumnNames))
	for col := range c.ColumnNames {
		for _, tp := range c.Config.Types {
			if tp.flag&c.PossibleTypes[col] != 0 {
				c.ColumnTypes[col] = tp.Name
				break
			}
		}
	}

	// Patch up enums
	for col, v := range c.ColumnTypes {
		if v != "enum" {
			continue
		}
		enumValues := []string{}
		for enumv := range c.Enums[col] {
			enumValues = append(enumValues, "  "+quoteLiteral(enumv))
		}
		enumName := c.ColumnNames[col] + "_enum"
		c.EnumDDL = c.EnumDDL + fmt.Sprintf("create type %s as enum (\n%s\n);\n\n", QuoteIdent(enumName), strings.Join(enumValues, ",\n"))
		c.ColumnTypes[col] = enumName
	}

	// Generate create table statement
	rows := []string{}
	for i, v := range c.ColumnTypes {
		nn := " not null"
		if c.Null[i] {
			nn = ""
		}
		rows = append(rows, fmt.Sprintf("  %s %s%s", QuoteName(c.ColumnNames[i]), QuoteIdent(v), nn))
	}
	c.TableDDL = fmt.Sprintf("create table %s (\n%s\n);\n", QuoteName(c.Config.TableName), strings.Join(rows, ",\n"))

	return nil
}

func (c *Intuitor) handleRow(row []string) error {
	for col, v := range row {
		if v == "" && c.Config.NotNull {
			// Empty column
			c.Null[col] = true
			continue
		}
		possibles := c.PossibleTypes[col]
		for _, tp := range c.Config.Types {
			if tp.flag&possibles != 0 {
				if !tp.Handle(v, col, c) {
					possibles = possibles &^ tp.flag
				}
			}
		}
		// fmt.Printf("%d,%d: %s -> %x\n", c.row, col, v, possibles)
		c.PossibleTypes[col] = possibles
	}
	return nil
}

var nonAlphaRe = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// TODO(steve): Consider handling camelcase here?
func snake(s string) string {
	words := nonAlphaRe.Split(s, -1)
	return strings.ToLower(strings.Join(words, "_"))
}

var needsQuoteRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// QuoteIdent attempts to quote a postgresql identifier
func QuoteIdent(s string) string {
	if needsQuoteRe.MatchString(s) {
		return s
	}
	return `"` + strings.Replace(s, `"`, `""`, -1) + `"`
}

// QuoteName attempts to quote a postgresql identifier or keyword
func QuoteName(s string) string {
	_, isKeyword := keywords[s]
	if !isKeyword && needsQuoteRe.MatchString(s) {
		return s
	}
	return `"` + strings.Replace(s, `"`, `""`, -1) + `"`
}

func quoteLiteral(s string) string {
	return `'` + strings.Replace(s, `'`, `''`, -1) + `'`
}
