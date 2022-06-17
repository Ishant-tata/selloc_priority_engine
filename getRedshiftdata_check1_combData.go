package main

import (
	"fmt"

	"github.com/go-gota/gota/dataframe"
	_ "github.com/lib/pq"
)

var comb1_check1_row []comb1_check1

type comb1_check1 struct {
	Prdct_key    string
	Listing_id   string
	Seller_id    string
	Transactions string
	No_of_days   string
}

func getRedshiftdata_check1_combData() dataframe.DataFrame {
	dbname := "dldb"
	check1 := "SELECT prdct_key,listing_id,p.seller_id,count(distinct transaction_id) as transactions,count(distinct date(order_date)) as no_of_days FROM tuldlmrt_mvp3.order_fact OF LEFT JOIN tuldlmrt.product_dim p ON p.prdct_key = of.product_key where DATE (order_Date) BETWEEN dateadd(DAY,-30,CURRENT_DATE) AND dateadd(DAY,-1,CURRENT_DATE) and prdct_key is not null group by 1,2,3"
	//sqler1, _ := ioutil.ReadFile("check1_query")
	db1 := redshift_connection(dbname)
	stmt1, _ := db1.Prepare(string(check1))
	defer stmt1.Close()
	datax1, err := stmt1.Query()
	if err != nil {
		fmt.Println(err)
	}
	defer datax1.Close()
	for datax1.Next() {
		var c1 comb1_check1
		if err := datax1.Scan(&c1.Prdct_key, &c1.Listing_id, &c1.Seller_id, &c1.Transactions, &c1.No_of_days); err != nil {
			fmt.Println(err)
		}
		comb1_check1_row = append(comb1_check1_row, c1)
	}
	df_ans := dataframe.LoadStructs(comb1_check1_row)
	return df_ans
}
