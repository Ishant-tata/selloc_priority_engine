package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
)

var (
	User string
	Pass string
	Hst  string
)

func redshift_connection(dbname string) *sql.DB {
	f, err := os.Open("redshift_credentials.csv")
	if err != nil {
		fmt.Println(err)
	}
	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	for i, line := range data {
		if i > 0 {
			for j, val := range line {
				if j == 0 {
					User = val
				} else if j == 1 {
					Pass = val
				} else {
					Hst = val
				}
			}
		}
	}
	var port string
	port = "5439"
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", Hst, port, User, Pass, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	return db
}
