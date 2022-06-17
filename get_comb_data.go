package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

/***		DEPENDENCY FILES
1. getRedshiftdata_check1_max_sale_per_day_combData.go
2. getRedshiftdata_check1_combData.go
3. getRedshiftdata_query_list_comb.go
4. redshift_connection.go
5. drop_duplicates_comb
6. getRedshiftData_base_query_comb.go

****/
/*
func main() {
	getCombdata()
}*/
func check1_data_prdct_unique(check1_data dataframe.DataFrame) []string {
	sel1 := check1_data.Col("Prdct_key").Records()
	hm := make(map[string]int, 0)
	for i := 0; i < len(sel1); i++ {
		if hm == nil {
			hm[sel1[i]] = i
		} else {
			_, check := hm[sel1[i]]
			if check == false {
				hm[sel1[i]] = i
			}
		}
	}
	sel2 := make([]string, 0)
	for key, _ := range hm {
		sel2 = append(sel2, key)
	}
	return sel2
}
func getCombdata() {
	//check1 := "SELECT prdct_key,listing_id,p.seller_id,count(distinct transaction_id) as transactions,count(distinct date(order_date)) as no_of_days FROM tuldlmrt_mvp3.order_fact OF LEFT JOIN tuldlmrt.product_dim p ON p.prdct_key = of.product_key where DATE (order_Date) BETWEEN dateadd(DAY,-30,CURRENT_DATE) AND dateadd(DAY,-1,CURRENT_DATE) and prdct_key is not null group by 1,2,3"
	//check1_max_sale_per_day = "SELECT prdct_key,listing_id,seller_id,max(transactions) as transactions_max from (SELECT prdct_key,listing_id,p.seller_id,date(order_date) as order_date,count(distinct transaction_id) as transactions FROM tuldlmrt_mvp3.order_fact OF LEFT JOIN tuldlmrt.product_dim p ON p.prdct_key = of.product_key where DATE (order_Date) BETWEEN dateadd(DAY,-30,CURRENT_DATE) AND dateadd(DAY,-1,CURRENT_DATE) and prdct_key is not null group by 1,2,3,4) group by 1,2,3"
	check1_data := getRedshiftdata_check1_combData()
	//fmt.Println(check1_data) //output cames and matches also within 1min.

	check1_max_sale_per_day := getRedshiftdata_check1_max_sale_per_day_combData()
	//fmt.Println(check1_max_sale_per_day) //output came and matches with python output.

	// After Left Join
	check1_data = check1_data.LeftJoin(check1_max_sale_per_day, "Prdct_key", "Listing_id", "Seller_id")
	//fmt.Println(check1_data) //it takes time around 1hr. for output shown.

	/*** below is output after join
		[260926x6] DataFrame

	    Prdct_key Listing_id        Seller_id Transactions No_of_days ...
	 0: 12227307  MP000000010432674 126340    16           15         ...
	 1: 5388941   MP000000000128562 100024    24           17         ...
	 3: 6848854   MP000000006366175 100058    55           26         ...
	 4: 13782037  MP000000011727077 126789    34           19         ...
	 5: 5982143   MP000000004884177 100213    2            2          ...
	 6: 11584114  MP000000007325329 124055    2            1          ...
	 7: 14696724  MP000000012452861 126743    2            2          ...
	 8: 14544881  MP000000012344200 127224    6            4          ...
	 9: 6767749   MP000000006272905 123762    23           12         ...
	    ...       ...               ...       ...          ...        ...
	    <string>  <string>          <string>  <string>     <string>   ...

	Not Showing: Transactions_max <string>
	*/

	// below is check1_data['Sale_Rate']=check1_data['transactions']/check1_data['no_of_days']
	sel1 := make([]float64, 0)
	sel2 := check1_data.Col("Transactions").Records()
	sel3 := check1_data.Col("No_of_days").Records()
	for i := 0; i < len(sel2); i++ {
		x, _ := strconv.Atoi(sel2[i])
		y, _ := strconv.Atoi(sel3[i])
		z := float64(x / y)
		sel1 = append(sel1, z)
	}
	check1_data = check1_data.Mutate(
		series.New(sel1, series.Float, "Sale_Rate"),
	)

	// check1_data_prdct_key = list(check1_data['prdct_key'].astype('int').unique())
	check1_data_prdct_key := check1_data_prdct_unique(check1_data)

	fmt.Println("Length of check1_data_prdct_key: ", len(check1_data_prdct_key)) // till this output is same. rest is checking.

	// replace {prdct_key} with %s.
	check2 := "SELECT i.slr_key,UPPER(prdct_key) prdct_key,slave_id,seller_id,case when inventory_available-4 >= 0 then inventory_available - 3 else 0 end as inventory_available FROM (select distinct A.slr_key,A.prdct_key,B.slave_id,C.seller_id,inventory_available  from tuldlmrt.inventory_fact A inner join tuldlmrt.slave_dim B on A.slv_key =B.slv_key inner join tuldlmrt.seller_dim C on A.slr_key= C.slr_key inner join tuldlmrt.product_dim D on A.prdct_key = D.prdct_key and C.seller_id= D.seller_id inner join tuldlmrt.product_listing_history E on A.prdct_key=E.prdct_key where current_date between A.effective_start_date and A.effective_end_date and A.active_record_flag =1 and current_date between B.effective_start_date and B.effective_end_date and B.active_record_flag =1 and B.slave_listing_status='LISTED_ACTIVE' and current_date between C.effective_start_date and C.effective_end_date and C.active_record_flag =1 and C.seller_listing_status='Is Listing and order process' and current_date between D.ussid_start_date_in_source and D.ussid_end_date_in_source and E.listing_status =1 and current_date between E.effective_start_date and E.effective_end_date )i where prdct_key in %s"
	temp := strings.Join(check1_data_prdct_key, ",")
	temp = "(" + temp + ")"
	query_list_val := []string{fmt.Sprintf(check2, temp)}
	//fmt.Println(query_list_val[0])

	check2_data := getRedshiftdata_query_list_comb(query_list_val[0], "dldb")
	check2_data = drop_duplicates_comb(check2_data)
	//fmt.Println(check2_data) //till line no.112

	check3_data := check1_data.LeftJoin(check2_data, "Seller_id", "Prdct_key")
	//fmt.Println("check3_data is :", check3_data)

	//check3_data['fulfillment'] = check3_data[['Sale_Rate','inventory_available']].min(axis=1)
	fullfillment_slice := make([]string, 0)
	for i := 0; i < len(check3_data.Col("Sale_rate").Records()); i++ {
		x := check3_data.Col("Sale_rate").Records()[i]
		y := check3_data.Col("inventory_available").Records()[i]
		float_x, _ := strconv.ParseFloat(x, 8)
		float_y, _ := strconv.ParseFloat(y, 8)
		temp := strconv.FormatFloat(math.Min(float_x, float_y), 'E', -1, 64)
		fullfillment_slice = append(fullfillment_slice, temp)
	}
	check3_data = check3_data.Mutate(
		series.New(fullfillment_slice, series.String, "Fulfillment"),
	)

	// check3_data['fulfillment_max'] = check3_data[['transactions_max','inventory_available']].min(axis=1)
	fullfillment_max_slice := make([]string, 0)
	for i := 0; i < len(check3_data.Col("Transactions_max").Records()); i++ {
		x := check3_data.Col("Transactions_max").Records()[i]
		y := check3_data.Col("inventory_available").Records()[i]
		float_x, _ := strconv.ParseFloat(x, 8)
		float_y, _ := strconv.ParseFloat(y, 8)
		temp := strconv.FormatFloat(math.Min(float_x, float_y), 'E', -1, 64)
		fullfillment_max_slice = append(fullfillment_max_slice, temp)
	}
	check3_data = check3_data.Mutate(
		series.New(fullfillment_max_slice, series.String, "Fulfillment_max"),
	)

	fmt.Println(check3_data)

	base_query_data := getRedshiftData_base_query_comb("dldb")
	fmt.Println("The base_query_data is: ", base_query_data)

}
