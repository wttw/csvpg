package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/wttw/csvpg"
)

func main() {
	config := csvpg.NewConfig()
	columnNames := ""
	excludeTypes := ""
	enums := false
	copy := true
	flag.BoolVar(&config.SnakeCase, "snake", true, "convert field names to snake_case")
	flag.IntVar(&config.Sample, "sample", 0, "consider only this many rows of the input")
	flag.StringVar(&columnNames, "columns", "", "comma-separated list of column names")
	flag.BoolVar(&config.ReadHeader, "header", true, "treat the first row of the file as a header")
	flag.StringVar(&excludeTypes, "exclude", "", "exclude this comma-separated list of SQL types from consideration")
	flag.StringVar(&config.TableName, "table", "", "name of the generated table")
	flag.BoolVar(&enums, "enums", false, "consider enum as a type")
	flag.BoolVar(&config.NotNull, "notnull", true, "check for not null")
	flag.BoolVar(&copy, "copy", true, "include psql \\copy command")
	flag.Parse()

	config.ColumnNames = strings.Split(columnNames, ",")
	config.Exclude = strings.Split(excludeTypes, ",")
	if !enums {
		config.EnumCount = 0
		config.EnumLength = 0
		config.Exclude = append(config.Exclude, "enum")
	}

	args := flag.Args()
	if len(args) != 1 {
		log.Fatalf("A single CSV file must be provided")
	}

	file, err := os.Open(args[0])
	if err != nil {
		log.Fatalf("Failed to open '%s': %s\n", args[0], err.Error())
	}

	if config.TableName == "" {
		basename := filepath.Base(args[0])
		config.TableName = strings.ToLower(strings.TrimSuffix(basename, filepath.Ext(basename)))
	}

	reader := csv.NewReader(file)

	it := csvpg.NewIntuitor(&config)

	err = it.Intuit(reader)
	if err != nil {
		log.Fatalf("Failed to intuit file: %s\n", err.Error())
	}

	fmt.Println(it.EnumDDL)
	fmt.Println(it.TableDDL)
	if copy {
		params := []string{"format csv"}
		if config.ReadHeader {
			params = append(params, "header")
		}
		fmt.Printf("\\copy %s from %s (%s)\n", csvpg.QuoteIdent(it.Config.TableName), args[0], strings.Join(params, ", "))
	}
}
