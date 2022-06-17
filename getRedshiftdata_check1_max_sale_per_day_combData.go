package main

import (
	"fmt"

	"github.com/go-gota/gota/dataframe"
)

var Check1_max_sale []check1_max_combData

type check1_max_combData struct {
	Prdct_key        string
	Listing_id       string
	Seller_id        string
	Transactions_max string
}

func getRedshiftdata_check1_max_sale_per_day_combData() dataframe.DataFrame {
	dbname := "dldb"
	check1_max_sale_per_day_query := "SELECT prdct_key,listing_id,seller_id,max(transactions) as transactions_max from (SELECT prdct_key,listing_id,p.seller_id,date(order_date) as order_date,count(distinct transaction_id) as transactions FROM tuldlmrt_mvp3.order_fact OF LEFT JOIN tuldlmrt.product_dim p ON p.prdct_key = of.product_key where DATE (order_Date) BETWEEN dateadd(DAY,-30,CURRENT_DATE) AND dateadd(DAY,-1,CURRENT_DATE) and prdct_key is not null group by 1,2,3,4) group by 1,2,3"
	db1 := redshift_connection(dbname)
	stmt1, _ := db1.Prepare(string(check1_max_sale_per_day_query))
	defer stmt1.Close()
	datax1, err := stmt1.Query()
	if err != nil {
		fmt.Println(err)
	}
	for datax1.Next() {
		var c2 check1_max_combData
		if err := datax1.Scan(&c2.Prdct_key, &c2.Listing_id, &c2.Seller_id, &c2.Transactions_max); err != nil {
			fmt.Println(err)
		}
		Check1_max_sale = append(Check1_max_sale, c2)
	}
	df := dataframe.LoadStructs(Check1_max_sale)
	return df
}
