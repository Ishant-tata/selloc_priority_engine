package main

import (
	"fmt"

	"github.com/go-gota/gota/dataframe"
)

var query_list_comb1 []query_list_comb

type query_list_comb struct {
	Slr_key             string
	Prdct_key           string
	Slave_id            string
	Seller_id           string
	Inventory_available string
}

func getRedshiftdata_query_list_comb(s string, dbname string) dataframe.DataFrame {
	db := redshift_connection(dbname)
	stmt2, _ := db.Prepare(s)
	defer stmt2.Close()
	datax2, err := stmt2.Query()
	if err != nil {
		panic(err)
	}
	for datax2.Next() {
		var c3 query_list_comb
		if err := datax2.Scan(&c3.Slr_key, &c3.Prdct_key, &c3.Slave_id, &c3.Seller_id, &c3.Inventory_available); err != nil {
			fmt.Println(err)
		}
		query_list_comb1 = append(query_list_comb1, c3)
	}
	df := dataframe.LoadStructs(query_list_comb1)
	return df
}
