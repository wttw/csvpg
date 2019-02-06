package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx"
)

func main() {

	file, err := os.Create("keywords.go")
	if err != nil {
		log.Fatalln(err)
	}

	cfg := pgx.ConnConfig{}
	db, err := pgx.Connect(cfg)
	if err != nil {
		log.Fatalln(err)
	}
	rows, err := db.Query(`select word, catcode::text from pg_get_keywords() where catcode != 'U'`)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Fprintf(file, "package csvpg\n\nvar keywords = map[string]string{\n")
	for rows.Next() {
		var k, v string
		err = rows.Scan(&k, &v)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Fprintf(file, "  \"%s\": \"%s\",\n", k, v)
	}
	fmt.Fprintf(file, "}\n")
}
