package main

//  OUTPUT COMES- LESS THAN 1MIN.
// output: 12121x3
// [Seller_id,Slave_id,Pincode_dldb]
import (
	"fmt"
	"io/ioutil"

	"github.com/go-gota/gota/dataframe"
	_ "github.com/lib/pq"
)

var redshift_row_dldb []redshift_Rows_dldb

type redshift_Rows_dldb struct {
	Seller_id    string
	Slave_id     string
	Pincode_dldb string
}

/*
func main() {
	df := getRedshiftData_store_pincode_query_dldb()
	fmt.Println(df)
}*/
func getRedshiftData_store_pincode_query_dldb() dataframe.DataFrame {
	// Getting credentials from redshift_credentials.csv

	sqler, _ := ioutil.ReadFile("store_pincode_query_dldb")
	db := redshift_connection("dldb") //redshift_connection file
	stmt, _ := db.Prepare(string(sqler))
	defer stmt.Close()

	datax, err := stmt.Query()

	if err != nil {
		fmt.Println("Error", err)
	}
	defer datax.Close()

	for datax.Next() {
		var r1 redshift_Rows_dldb
		if err := datax.Scan(&r1.Seller_id, &r1.Slave_id, &r1.Pincode_dldb); err != nil {
			fmt.Println(err)
		}
		redshift_row_dldb = append(redshift_row_dldb, r1)
	}

	df := dataframe.LoadStructs(redshift_row_dldb)
	return df
}
