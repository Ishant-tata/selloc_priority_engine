package main

// output is came for sample data.
import (
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

/*
func main() {
	// sample dataframe for testing purpose only.
	df1 := dataframe.New(
		series.New([]string{"125052", "125051", "125032"}, series.String, "PINCODE_X"),
		series.New([]string{"103456", "213432", "574283"}, series.String, "PINCODE_Y"),
		series.New([]string{"10", "20", "30"}, series.String, "Distance"),
		series.New([]string{"MUMBAI", "NEW DELHI", "KOLKATA"}, series.String, "destination_city"),
		series.New([]string{"HARYANA", "PUNJAB", "GOA"}, series.String, "destination_state"),
		series.New([]string{"HISAR", "KOTA", "CHENNAI"}, series.String, "source_city"),
		series.New([]string{"KARNATAKA", "TAMIL NADU", "MIZORAM"}, series.String, "source_state"),
	)
	df := getZoneData(df1)
	fmt.Println(df)
}*/

///    LOCAL ZONE METHODS
func check_it_is_in_slice_2nd_cond_local(city string) bool {
	temp := make([]string, 0)
	temp = append(temp, "MUMBAI", "THANE", "NAVI MUMBAI", "KALYAN", "GREATER THANE")
	for i := 0; i < len(temp); i++ {
		if city == temp[i] {
			return true
		}
	}
	return false
}

func check_it_is_in_slice_3rd_cond_local(city string) bool {
	temp := make([]string, 0)
	temp = append(temp, "NOIDA", "GURGAON", "FARIDABAD", "GHAZIABAD")
	for i := 0; i < len(temp); i++ {
		if city == temp[i] {
			return true
		}
	}
	return false
}

//  METRO ZONE METHODS
func check_it_is_in_slice_1st_cond_metro(city string) bool {
	temp := make([]string, 0)
	temp = append(temp, "MUMBAI", "HYDERABAD", "BENGALURU", "NEW DELHI", "KOLKATA", "CHENNAI", "GURGAON", "PUNE", "AHMEDABAD")
	for i := 0; i < len(temp); i++ {
		if city == temp[i] {
			return true
		}
	}
	return false
}

//  SOURCE ZONE ALLOCATED METHODS
func check_it_is_in_slice_1st_cond_source_zone(state string) bool {
	temp := make([]string, 0)
	temp = append(temp, "MAHARASHTRA", "GUJARAT", "MADHYA PRADESH", "GOA", "DAMAN & DIU", "DADRA AND NAGAR HAVELI")
	for i := 0; i < len(temp); i++ {
		if state == temp[i] {
			return true
		}
	}
	return false
}
func check_it_is_in_slice_2nd_cond_source_zone(state string) bool {
	temp := make([]string, 0)
	temp = append(temp, "KERALA", "TAMIL NADU", "ANDHRA PRADESH", "TELANGANA", "KARNATAKA", "PONDICHERRY", "ANDAMAN & NICOBAR ISLANDS", "LAKSHADWEEP")
	for i := 0; i < len(temp); i++ {
		if state == temp[i] {
			return true
		}
	}
	return false
}
func check_it_is_in_slice_3rd_cond_source_zone(state string) bool {
	temp := make([]string, 0)
	temp = append(temp, "DELHI", "RAJASTHAN", "HARYANA", "UTTAR PRADESH", "UTTARAKHAND", "HIMACHAL PRADESH", "PUNJAB", "CHANDIGARH", "HIMACHALPRADESH")
	for i := 0; i < len(temp); i++ {
		if state == temp[i] {
			return true
		}
	}
	return false
}
func check_it_is_in_slice_4th_cond_source_zone(state string) bool {
	temp := make([]string, 0)
	temp = append(temp, "WEST BENGAL", "BIHAR", "JHARKHAND", "ORISSA", "ODISHA", "CHATTISGARH")
	for i := 0; i < len(temp); i++ {
		if state == temp[i] {
			return true
		}
	}
	return false
}
func check_it_is_in_slice_5th_cond_source_zone(state string) bool {
	temp := make([]string, 0)
	temp = append(temp, "ASSAM", "JAMMU & KASHMIR", "MIZORAM", "MEGHALAYA", "SIKKIM", "TRIPURA", "ARUNACHAL PRADESH", "MANIPUR", "NAGALAND", "JAMMUANDKASHMIR")
	for i := 0; i < len(temp); i++ {
		if state == temp[i] {
			return true
		}
	}
	return false
}
func getZoneData(df dataframe.DataFrame) dataframe.DataFrame {
	// columns of input df might be
	// 'PINCODE_X','PINCODE_Y','Distance','destination_city','destination_state','source_city','source_state'

	src_city_slice := make([]string, 0)
	dest_city_slice := make([]string, 0)
	src_state_slice := make([]string, 0)
	dest_state_slice := make([]string, 0)
	pincode_x_slice := make([]string, 0)
	pincode_y_slice := make([]string, 0)
	//Distance_slice := make([]string, 0)

	sel1 := df.Col("source_city")
	src_city_slice = sel1.Records()

	sel2 := df.Col("destination_city")
	dest_city_slice = sel2.Records()

	sel3 := df.Col("source_state")
	src_state_slice = sel3.Records()

	sel4 := df.Col("destination_state")
	dest_state_slice = sel4.Records()

	sel5 := df.Col("PINCODE_X")
	pincode_x_slice = sel5.Records()

	sel6 := df.Col("PINCODE_Y")
	pincode_y_slice = sel6.Records()

	//sel7 := df.Col("Distance")
	//Distance_slice = sel7.Records()

	length_of_df := len(src_city_slice)

	/******************   LOCAL ALLOCATED ZONE DIVIDE  ***********************/
	// create slice for local_allocated
	var local_allocated []int

	for i := 0; i < length_of_df; i++ {
		if (src_city_slice[i] == dest_city_slice[i] && len(src_city_slice[i]) != 0) ||
			(pincode_x_slice[i] == pincode_y_slice[i] && len(pincode_y_slice[i]) != 0) {
			local_allocated = append(local_allocated, 1)
		} else {
			local_allocated = append(local_allocated, 0)
		}
	}
	// 2nd condition of local_allocated

	for i := 0; i < length_of_df; i++ {
		if check_it_is_in_slice_2nd_cond_local(src_city_slice[i]) &&
			check_it_is_in_slice_2nd_cond_local(dest_city_slice[i]) {
			local_allocated[i] = 1
		} else {
			local_allocated[i] = local_allocated[i]
		}
	}
	// 3rd condition of local_allocated

	for i := 0; i < length_of_df; i++ {
		if check_it_is_in_slice_3rd_cond_local(src_city_slice[i]) &&
			check_it_is_in_slice_3rd_cond_local(dest_city_slice[i]) {
			local_allocated[i] = 1
		} else {
			local_allocated[i] = local_allocated[i]
		}
	}

	/**************************  METRO  ZONE ALLOCATED  ******************************/

	//  1st condition
	var metro_allocated []int
	for i := 0; i < length_of_df; i++ {
		if check_it_is_in_slice_1st_cond_metro(src_city_slice[i]) &&
			check_it_is_in_slice_1st_cond_metro(dest_city_slice[i]) {
			metro_allocated = append(metro_allocated, 1)
		} else {
			metro_allocated = append(metro_allocated, 0)
		}
	}
	// 2nd condition
	for i := 0; i < length_of_df; i++ {
		if src_city_slice[i] == "MUMBAI" && dest_city_slice[i] == "MUMBAI" {
			metro_allocated[i] = 0
		} else {
			metro_allocated[i] = metro_allocated[i]
		}
	}
	// 3rd condition
	for i := 0; i < length_of_df; i++ {
		if src_city_slice[i] == "NEW DELHI" && dest_city_slice[i] == "NEW DELHI" {
			metro_allocated[i] = 0
		} else {
			metro_allocated[i] = metro_allocated[i]
		}
	}

	/********************************** SOURCE ZONE ALLOCATED  ****************************/

	source_zone_allocated := make([]string, length_of_df)
	for i := 0; i < length_of_df; i++ {
		if check_it_is_in_slice_1st_cond_source_zone(src_state_slice[i]) {
			source_zone_allocated[i] = "WEST"
		} else if check_it_is_in_slice_2nd_cond_source_zone(src_state_slice[i]) {
			source_zone_allocated[i] = "SOUTH"
		} else if check_it_is_in_slice_3rd_cond_source_zone(src_state_slice[i]) {
			source_zone_allocated[i] = "NORTH"
		} else if check_it_is_in_slice_4th_cond_source_zone(src_state_slice[i]) {
			source_zone_allocated[i] = "EAST"
		} else if check_it_is_in_slice_5th_cond_source_zone(src_state_slice[i]) {
			source_zone_allocated[i] = "NE & JK"
		}
	}

	/*********************************** SHIPPING ZONE *********************************/
	shipping_zone := make([]string, length_of_df)
	for i := 0; i < length_of_df; i++ {
		if check_it_is_in_slice_1st_cond_source_zone(dest_state_slice[i]) {
			shipping_zone[i] = "WEST"
		} else if check_it_is_in_slice_2nd_cond_source_zone(dest_state_slice[i]) {
			shipping_zone[i] = "SOUTH"
		} else if check_it_is_in_slice_3rd_cond_source_zone(dest_state_slice[i]) {
			shipping_zone[i] = "NORTH"
		} else if check_it_is_in_slice_4th_cond_source_zone(dest_state_slice[i]) {
			shipping_zone[i] = "EAST"
		} else if check_it_is_in_slice_5th_cond_source_zone(dest_state_slice[i]) {
			shipping_zone[i] = "NE & JK"
		}
	}

	/*********************************  WITHIN ZONE ALLOCATED  *******************************/
	within_zone_allocated := make([]int, length_of_df)
	for i := 0; i < length_of_df; i++ {
		if local_allocated[i] == 0 && metro_allocated[i] == 0 &&
			source_zone_allocated[i] != " " && source_zone_allocated[i] == shipping_zone[i] {
			within_zone_allocated[i] = 1
		} else {
			within_zone_allocated[i] = 0
		}
	}

	/********************************* NORTH EAST AND JAMMU KASHMIR ZONE **********************/
	NE_JK_Allocated := make([]int, length_of_df)
	for i := 0; i < length_of_df; i++ {
		if local_allocated[i] == 0 && metro_allocated[i] == 0 && within_zone_allocated[i] == 0 &&
			check_it_is_in_slice_5th_cond_source_zone(dest_state_slice[i]) {
			NE_JK_Allocated[i] = 1
		} else {
			NE_JK_Allocated[i] = 0
		}
	}

	/***************************************** ROI ALLOCATED  *************************************/
	ROI_ALLOCATED := make([]int, length_of_df)
	for i := 0; i < length_of_df; i++ {
		if len(src_state_slice[i]) != 0 && len(dest_state_slice[i]) != 0 &&
			(local_allocated[i]+metro_allocated[i]+within_zone_allocated[i]+NE_JK_Allocated[i] == 0) {
			ROI_ALLOCATED[i] = 1
		} else {
			ROI_ALLOCATED[i] = 0
		}
	}

	/**************************************  FINAL COLUMN- ZONE ALLOCATED  *************************/
	zone_allocated := make([]string, length_of_df)
	for i := 0; i < length_of_df; i++ {
		if local_allocated[i] == 1 {
			zone_allocated[i] = "LOCAL"
		} else if metro_allocated[i] == 1 {
			zone_allocated[i] = "METRO"
		} else if within_zone_allocated[i] == 1 {
			zone_allocated[i] = "WITHIN ZONE"
		} else if NE_JK_Allocated[i] == 1 {
			zone_allocated[i] = "NE & JK"
		} else if ROI_ALLOCATED[i] == 1 {
			zone_allocated[i] = "ROI"
		}
	}
	// FINALLY APPENDING zone_allocated slice to initial_dataframe
	mut := df.Mutate(
		series.New(zone_allocated, series.String, "ZONE_ALLOCATED"),
	)
	return mut
}
