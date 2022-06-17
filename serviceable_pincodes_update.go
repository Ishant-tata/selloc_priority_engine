package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func unique_seller_unique_pincode(temp []string) []string {
	//slave_id := df.Col("Slave_id").Records()
	hm := make(map[string]int, 0)
	for i := 0; i < len(temp); i++ {
		_, check := hm[temp[i]]
		if check == true {
			continue
		} else {
			hm[temp[i]] = i
		}
	}
	var ntemp []string
	for key, _ := range hm {
		ntemp = append(ntemp, key)
	}
	return ntemp
}
func seller_unique_drop_duplicates(pincode_x []string) []string {
	hm := make(map[string]int, 0)
	for i := 0; i < len(pincode_x); i++ {
		_, check := hm[pincode_x[i]]
		if check == true {
			continue
		} else {
			hm[pincode_x[i]] = i
		}
	}
	var ans []string
	for key, _ := range hm {
		ans = append(ans, key)
	}
	return ans
}
func make_source_pincode_list() []string {
	csfile, err := os.Open("source_pincodes.csv")

	if err != nil {

		log.Fatal(err)

	}

	defer csfile.Close()

	lanedb := dataframe.ReadCSV(csfile)
	var ans []string
	ans = lanedb.Col("Pincodes").Records()
	return ans
}
func equivalent_setdiff1d(sel1 []string, sel2 []string) []string {
	var ans []string
	hm2 := make(map[string]int, 0)
	hm1 := make(map[string]int, 0)
	for i := 0; i < len(sel2); i++ {
		hm2[sel2[i]] = i
	}
	for i := 0; i < len(sel1); i++ {
		_, check := hm2[sel1[i]]
		if check == false {
			_, check2 := hm1[sel1[i]]
			if check2 == false {
				ans = append(ans, sel1[i])
				hm1[sel1[i]] = i
			}
		}
	}
	return ans
}
func readcsvfile(path string) dataframe.DataFrame {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	rec, err := csvReader.ReadAll()
	if err != nil {
		panic(err)
	}
	df := dataframe.LoadRecords(rec)
	return df
}
func isfound(s string, sel []string) (int, bool) {
	for i := 0; i < len(sel); i++ {
		if sel[i] == s {
			return i, true
		}
	}
	return -1, false
}
func remove_element_from_slice(a []string, i int) {
	copy(a[i:], a[i+1:]) // Shift a[i+1:] left one index.
	a[len(a)-1] = ""     // Erase last element (write zero value).
	a = a[:len(a)-1]     // Truncate slice.
}
func perform_outer_join(sel1 []string, sel2 []string) dataframe.DataFrame {
	var sel1_ans []string
	var sel2_ans []string
	for i := 0; i < len(sel1); i++ {
		ok, check := isfound(sel1[i], sel2) //ok is the index where sel1[i] is found in sel2.
		if check == true {
			sel1_ans = append(sel1_ans, sel1[i])
			sel2_ans = append(sel2_ans, sel2[ok])
			remove_element_from_slice(sel1, i)
			remove_element_from_slice(sel2, ok)
		} else {
			sel1_ans = append(sel1_ans, sel1[i])
			sel2_ans = append(sel2_ans, "nil")
		}
	}
	for i := 0; i < len(sel2); i++ {
		ok, check := isfound(sel2[i], sel1) //ok is the index where sel2[i] is found in sel1.
		if check == true {
			sel1_ans = append(sel1_ans, sel1[ok])
			sel2_ans = append(sel2_ans, sel2[i])
			remove_element_from_slice(sel1, ok)
			remove_element_from_slice(sel2, i)
		} else {
			sel1_ans = append(sel1_ans, sel2[i])
			sel2_ans = append(sel2_ans, "nil")
		}
	}
	df_ans := dataframe.New(
		series.New(sel1_ans, series.String, "Pincode_master"),
		series.New(sel2_ans, series.String, "Pincode"),
	)
	return df_ans
}
func drop_duplicates_df1(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("PINCODE").Records()
	sel2 := df.Col("Pincode_master").Records()
	sel3 := df.Col("Lat").Records()
	sel4 := df.Col("Long").Records()
	hm := make(map[string]int, 0)
	var df_ans dataframe.DataFrame
	var sel1_ans []string
	var sel2_ans []string
	var sel3_ans []string
	var sel4_ans []string
	for i := 0; i < len(sel1); i++ {
		temp := sel1[i] + "=" + sel2[i] + "=" + sel3[i] + "=" + sel4[i]
		_, check := hm[temp]
		if check == false {
			hm[temp] = i
			sel1_ans = append(sel1_ans, sel1[i])
			sel2_ans = append(sel2_ans, sel2[i])
			sel3_ans = append(sel3_ans, sel3[i])
			sel4_ans = append(sel4_ans, sel4[i])
		}
	}
	df_ans = dataframe.New(
		series.New(sel1_ans, series.String, "PINCODE"),
		series.New(sel2_ans, series.String, "Pincode_master"),
		series.New(sel3_ans, series.String, "Lat"),
		series.New(sel4_ans, series.String, "Long"),
	)
	return df_ans
}
func isnull_df1_Lat(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("PINCODE").Records()
	sel2 := df.Col("Pincode_master").Records()
	sel3 := df.Col("Lat").Records()
	sel4 := df.Col("Long").Records()
	var df_ans dataframe.DataFrame
	var sel1_ans []string
	var sel2_ans []string
	var sel3_ans []string
	var sel4_ans []string
	for i := 0; i < len(sel3); i++ {
		if sel3[i] == "NaN" {
			sel1_ans = append(sel1_ans, sel1[i])
			sel2_ans = append(sel2_ans, sel2[i])
			sel3_ans = append(sel3_ans, sel3[i])
			sel4_ans = append(sel4_ans, sel4[i])
		}
	}
	df_ans = dataframe.New(
		series.New(sel1_ans, series.String, "PINCODE"),
		series.New(sel2_ans, series.String, "Pincode_master"),
		series.New(sel3_ans, series.String, "Lat"),
		series.New(sel4_ans, series.String, "Long"),
	)
	return df_ans
}
func check_lat_and_pincodemaster_notnull(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("PINCODE").Records()
	sel2 := df.Col("Pincode_master").Records()
	sel3 := df.Col("Lat").Records()
	sel4 := df.Col("Long").Records()
	var df_ans dataframe.DataFrame
	var sel1_ans []string
	var sel2_ans []string
	var sel3_ans []string
	var sel4_ans []string
	for i := 0; i < len(sel3); i++ {
		if sel3[i] != "NaN" && sel2[i] != "NaN" {
			sel1_ans = append(sel1_ans, sel1[i])
			sel2_ans = append(sel2_ans, sel2[i])
			sel3_ans = append(sel3_ans, sel3[i])
			sel4_ans = append(sel4_ans, sel4[i])
		}
	}
	df_ans = dataframe.New(
		series.New(sel1_ans, series.String, "PINCODE"),
		series.New(sel2_ans, series.String, "Pincode_master"),
		series.New(sel3_ans, series.String, "Lat"),
		series.New(sel4_ans, series.String, "Long"),
	)
	return df_ans
}
func pincode_map_lambda(df dataframe.DataFrame, s string, x int) []string {
	var ans []string
	sel := df.Col(s).Records()
	for i := 0; i < len(sel); i++ {
		ans = append(ans, sel[i][:x])
	}
	return ans
}
func updateServiceablePincodes(zone_city_pincode_mapping_data_2 dataframe.DataFrame) {
	//seller_unique_pincodes := "select distinct slave_id,pincode as pincode_x from unistoremart.slave_dim where is_listing = 1 and pincode != '' and pincode is not null"
	seller_unique_pincodes_data := pincodeSlaveMappingImpute()
	seller_unique_pincodes_data = dataframe.New(
		series.New(seller_unique_pincodes_data.Col("Slave_id").Records(), series.String, "Slave_id"),
		series.New(seller_unique_pincodes_data.Col("Final_Pincode"), series.String, "Pincode_x"),
	)
	//unique_slave_list := unique_seller_unique_pincode(seller_unique_pincodes_data.Col("Slave_id").Records())
	relevant_pincode_data := seller_unique_drop_duplicates(seller_unique_pincodes_data.Col("Pincode_x").Records())
	rel_pincode_list := unique_seller_unique_pincode(relevant_pincode_data)
	//fmt.Println("Unique_slave_list", unique_slave_list)
	fmt.Println("Length of rel_pincode_list", len(rel_pincode_list)) //1531

	source_pincode_list := make_source_pincode_list() // length is 1526
	source_diff_list := equivalent_setdiff1d(rel_pincode_list, source_pincode_list)
	fmt.Println("source_diff_list", source_diff_list)
	fmt.Println("length of source diff list", len(source_diff_list))

	zone_city_pincode_mapping_data_2 = zone_city_pincode_mapping_data_2.Mutate(
		series.New(zone_city_pincode_mapping_data_2.Col("Pincode").Records(), series.String, "Pincode_master"),
	)
	df := readcsvfile("Lat_Long_File.csv")
	df1 := perform_outer_join(zone_city_pincode_mapping_data_2.Col("Pincode_master").Records(), df.Col("PINCODE").Records())
	df1 = df1.Mutate(
		series.New(df.Col("Lat").Records(), series.String, "Lat"),
	)
	df1 = df1.Mutate(
		series.New(df.Col("Long").Records(), series.String, "Long"),
	)
	fmt.Println(df1) //come from outer join of zone_city_maping and lat_long_file.csv
	df1 = dataframe.New(
		series.New(df1.Col("Pincode_master"), series.String, "PINCODE"),
		series.New(df1.Col("Pincode_master"), series.String, "Pincode_master"),
		series.New(df1.Col("Lat"), series.String, "Lat"),
		series.New(df1.Col("Long"), series.String, "Long"),
	)
	df1 = drop_duplicates_df1(df1)
	df1_nan := isnull_df1_Lat(df1)                                          //returns a dataframe that contains only rows where df1['Lat'] isnull or nil.
	fmt.Println(df1_nan)                                                    //[PINCODE,pincode_master,Lat,Long]
	pincode_check_complete_data := check_lat_and_pincodemaster_notnull(df1) // return three columns (pincode_master,lat,long) of df1 where df1[lat].notnull and df1[pincodemaster].notnull
	fmt.Println("pincode_check_complete_data", pincode_check_complete_data)
	join_key_df1_nan := pincode_map_lambda(df1_nan, "PINCODE", 3)
	df1_nan = df1_nan.Mutate(
		series.New(join_key_df1_nan, series.String, "join_key"),
	)
	join_key_pincode_complete := pincode_map_lambda(pincode_check_complete_data, "Pincode_master", 3)
	pincode_check_complete_data = pincode_check_complete_data.Mutate(
		series.New(join_key_pincode_complete, series.String, "join_key"),
	)
	join_key2_df1_nan := pincode_map_lambda(df1_nan, "PINCODE", 1)
	df1_nan = df1_nan.Mutate(
		series.New(join_key2_df1_nan, series.String, "join_key_2"),
	)
	join_key2_pincode_complete := pincode_map_lambda(pincode_check_complete_data, "Pincode_master", 1)
	pincode_check_complete_data = pincode_check_complete_data.Mutate(
		series.New(join_key2_pincode_complete, series.String, "join_key_2"),
	)
	df1_nan_check := df1_nan.LeftJoin(pincode_check_complete_data, "join_key")

}
