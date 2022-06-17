package main

import (
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

type check struct {
	slave_id      []string
	seller_id     []string
	final_pincode []string
}

func pincodeSlaveMappingImpute() dataframe.DataFrame {
	db1 := getRedshiftData_store_pincode_query_dwh()
	db2 := getRedshiftData_store_pincode_query_dldb()
	db := db1.OuterJoin(db2, "Slave_id", "Seller_id")
	//fmt.Println(db) // till this point output is same- py and go.
	db3 := fillna(db) //after fillna method

	// extracting useful columns
	db4 := dataframe.New(
		series.New(db3.Col("Slave_id"), series.String, "slave_id"),
		series.New(db3.Col("Seller_id"), series.String, "seller_id"),
		series.New(db3.Col("Final_Pincode"), series.String, "final_pincode"),
	)
	db5 := drop_duplicates(db4)
	return db5

	// A B C
	// A D C
	// A B C
	//

}

/*
func main() {
	fmt.Println("Pincode Slave Mapping Impute Call")
	pincodeSlaveMappingImpute()
}*/
func fillna(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("Pincode_dwh")
	sel2 := df.Col("Pincode_dldb")
	slice1 := make([]string, 0)

	slice2 := make([]string, 0)
	slice1 = sel1.Records()
	slice2 = sel2.Records()

	ans := make([]string, 0)
	for index := 0; index < len(slice1); index++ {
		if slice1[index] == "NaN" {
			ans = append(ans, slice2[index])
		} else {
			ans = append(ans, slice1[index])
		}
	}
	mut := df.Mutate(
		series.New(ans, series.String, "Final_Pincode"),
	)
	return mut
}

func drop_duplicates(df dataframe.DataFrame) dataframe.DataFrame {
	// making different-2 series for df columns
	series1 := df.Col("slave_id")
	series2 := df.Col("seller_id")
	series3 := df.Col("final_pincode")

	// make slice of length 0.
	slice1 := make([]string, 0)
	slice2 := make([]string, 0)
	slice3 := make([]string, 0)

	// make slice from series
	for _, val := range series1.Records() {
		slice1 = append(slice1, val)
	}
	for _, val := range series2.Records() {
		slice2 = append(slice2, val)
	}
	for _, val := range series3.Records() {
		slice3 = append(slice3, val)
	}
	slice4 := make([]string, 0)
	for i := 0; i < len(slice1); i++ {
		temp := slice1[i] + "[]" + slice2[i] + "[]" + slice3[i]
		slice4 = append(slice4, temp)
	}
	// Make Hashmap<string,integer>- integer shows index.
	hm := make(map[string]int, 0)
	// transverse slice4 and appending rows uniquely
	for idx, val := range slice4 {
		if hm == nil {
			hm[val] = idx
		} else {
			_, check := hm[val]
			if check == false {
				hm[val] = idx
			} else {
				continue
			}
		}
	}
	// Make new slices to store series of records.
	final_slice1 := make([]string, 0)
	final_slice2 := make([]string, 0)
	final_slice3 := make([]string, 0)

	// then make new slice of length 0.
	slice5 := make([]int, 0)
	// adding values of map to slice by using stored index.
	for _, val := range hm {
		slice5 = append(slice5, val)
	}
	// then finally make new dataframe
	for _, val := range slice5 {
		final_slice1 = append(final_slice1, slice1[val])
		final_slice2 = append(final_slice2, slice2[val])
		final_slice3 = append(final_slice3, slice3[val])
	}
	// adding final_slice to new_dataframe.
	new_df := dataframe.New(
		series.New(final_slice1, series.String, "Slave_id"),
		series.New(final_slice2, series.String, "Seller_id"),
		series.New(final_slice3, series.String, "Final_Pincode"),
	)
	// return new_dataframe.
	return new_df

}
