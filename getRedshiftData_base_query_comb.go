package main

import (
	"fmt"
	"io/ioutil"

	"github.com/go-gota/gota/dataframe"
)

type base_query_impl struct {
	Transaction_id              string
	Handed_over_to_courier_date string
	Seller_id                   string
}

var base_query1 []base_query_impl

func getRedshiftData_base_query_comb(dbname string) dataframe.DataFrame {
	base_query, _ := ioutil.ReadFile("base_query")
	db := redshift_connection(dbname)
	stmt, _ := db.Prepare(string(base_query))
	defer stmt.Close()
	datax, err := stmt.Query()
	if err != nil {
		panic(err)
	}
	for datax.Next() {
		var c1 base_query_impl
		if err := datax.Scan(&c1.Transaction_id, &c1.Handed_over_to_courier_date, &c1.Seller_id); err != nil {
			fmt.Println(err)
		}
		base_query1 = append(base_query1, c1)
	}
	df_new := dataframe.LoadStructs(base_query1)
	return df_new
}
