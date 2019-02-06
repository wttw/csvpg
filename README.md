# csvpg
Generate Postgresql tables from CSV files

```csvpg name.csv```

`csvpg` parses a CSV file and prints a PostgreSQL syntax "create table"
statement into which the CSV can be imported.

By default it assumes that the first row of the CSV file is a header,
and uses that to generate column names for the table. It then looks at
the contents of each column to find an appropriate SQL type for it.

PostgreSQL supports NULL columns in CSV input, denoted by a zero length
unquoted field. The CSV parser csvpg uses doesn't record whether a field
is quoted or not, but if a zero length field is found then that SQL
field will be generated as nullable.

There is untested support for generating enums for columns with a small
number of values in it, disabled by default.

## Usage

```
Usage of ./csvpg:
  -columns string
    	comma-separated list of column names
  -copy
    	include psql \copy command (default true)
  -enums
    	consider enum as a type
  -exclude string
    	exclude this comma-separated list of SQL types from consideration
  -header
    	treat the first row of the file as a header (default true)
  -notnull
    	check for not null (default true)
  -sample int
    	consider only this many rows of the input
  -snake
    	convert field names to snake_case (default true)
  -table string
    	name of the generated table
 ```
 
## Installation

With Go >= 1.11 installed, run "go build" from the cmd/csvpg subdirectory.

Or [download a pre-built binary](https://github.com/wttw/csvpg/releases)
