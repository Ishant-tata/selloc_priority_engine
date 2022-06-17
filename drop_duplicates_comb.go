package main

import (
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func drop_duplicates_comb(df dataframe.DataFrame) dataframe.DataFrame {
	var s string
	hm := make(map[string]int, 0)
	sel1 := make([]string, 0)
	sel2 := make([]string, 0)
	sel3 := make([]string, 0)
	sel4 := make([]string, 0)
	sel5 := make([]string, 0)

	sel1 = df.Col("Slr_key").Records()
	sel2 = df.Col("Prdct_key").Records()
	sel3 = df.Col("Slave_id").Records()
	sel4 = df.Col("Seller_id").Records()
	sel5 = df.Col("Inventory_available").Records()

	sel1_ans := make([]string, 0)
	sel2_ans := make([]string, 0)
	sel3_ans := make([]string, 0)
	sel4_ans := make([]string, 0)
	sel5_ans := make([]string, 0)

	for i := 0; i < len(df.Col("Slr_key").Records()); i++ {
		s = sel1[i] + "[]" + sel2[i] + "[]" + sel3[i] + "[]" + sel4[i] + "[]" + sel5[i]
		if hm == nil {
			hm[s] = i
		} else {
			_, check := hm[s]
			if check == false {
				hm[s] = i
			}
		}
	}
	for _, val := range hm {
		sel1_ans = append(sel1_ans, sel1[val])
		sel2_ans = append(sel2_ans, sel2[val])
		sel3_ans = append(sel3_ans, sel3[val])
		sel4_ans = append(sel4_ans, sel4[val])
		sel5_ans = append(sel5_ans, sel5[val])
	}
	mut := dataframe.New(
		series.New(sel1_ans, series.String, "Slr_key"),
		series.New(sel2_ans, series.String, "Prdct_key"),
		series.New(sel3_ans, series.String, "Slave_id"),
		series.New(sel4_ans, series.String, "Seller_id"),
		series.New(sel5_ans, series.String, "Inventory_available"),
	)
	return mut
}
