package main

// 12:45pm-1:15 run time
// 1:15-1:46    printing output

// column names [SELLER_CODE TRANSACTION_ID SLAVE_CODE REJECTION_TIMESTAMP ALLOCATION_TIMESTAMP] <nil>
import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"time"

	_ "github.com/go-sql-driver/mysql"
	drv "github.com/uber/athenadriver/go"
)

var Row []Rows

type Rows struct {
	SELLER_CODE          string
	TRANSACTION_ID       string
	SLAVE_CODE           string
	REJECTION_TIMESTAMP  string
	ALLOCATION_TIMESTAMP string
}

func athenaData() ([]Rows, error) {
	conn, err := drv.NewDefaultConfig(s3_staging_dir, region, access_key, secret_key)
	start_time := time.Now()

	db, err := sql.Open(drv.DriverName, conn.Stringify())
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected...", db)
	query, _ := ioutil.ReadFile("athena_query")
	//fmt.Println(string(query))
	stmt, _ := db.Prepare(string(query))
	defer stmt.Close()
	rows, _ := stmt.Query()
	defer rows.Close()
	fmt.Println("it is working..")
	for rows.Next() {
		var r Rows
		if err := rows.Scan(&r.SELLER_CODE, &r.TRANSACTION_ID, &r.SLAVE_CODE, &r.REJECTION_TIMESTAMP, &r.ALLOCATION_TIMESTAMP); err != nil {
			return Row, err
		}
		Row = append(Row, r)
	}
	if err != nil {
		fmt.Println(err)
		body := "Hi,The DFM Store Priority Python code has failed with error {}. Please check."
		send_mail("apagote@tataunistore.com", "Alert! DFM Store Priority Python Code not run! ", body)
	} else {
		end_time := time.Now()
		fmt.Printf("Athena Data fetched in %v seconds\n", end_time.Sub(start_time))
		//fmt.Println(string(query))
	}
	return Row, err
}
