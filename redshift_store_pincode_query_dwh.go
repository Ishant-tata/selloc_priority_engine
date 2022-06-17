package main

//  OUTPUT COMES- LESS THAN 1MIN.
import (
	"fmt"
	"io/ioutil"

	"github.com/go-gota/gota/dataframe"
	_ "github.com/lib/pq"
)

var redshift_row_dwh []redshift_Rows_dwh

type redshift_Rows_dwh struct {
	Seller_id   string
	Slave_id    string
	Pincode_dwh string
}

/*
func main() {
	df := getRedshiftData_store_pincode_query_dwh()
	fmt.Println(df)
}*/
func getRedshiftData_store_pincode_query_dwh() dataframe.DataFrame {
	// Getting credentials from redshift_credentials.csv

	sqler, _ := ioutil.ReadFile("store_pincode_query_dwh")
	db := redshift_connection("dwhtest")
	stmt, _ := db.Prepare(string(sqler))
	defer stmt.Close()

	datax, err := stmt.Query()

	if err != nil {
		fmt.Println("Error", err)
	}
	defer datax.Close()

	for datax.Next() {
		var r1 redshift_Rows_dwh
		if err := datax.Scan(&r1.Seller_id, &r1.Slave_id, &r1.Pincode_dwh); err != nil {
			fmt.Println(err)
		}
		redshift_row_dwh = append(redshift_row_dwh, r1)
	}

	df := dataframe.LoadStructs(redshift_row_dwh)
	return df
}
