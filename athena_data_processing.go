package main

import (
	"fmt"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func getAthenaProcessedData() {
	// df is coming from athena_data.go
	Row, err := athenaData()
	if err != nil {
		fmt.Println(err)
	}
	df := dataframe.LoadStructs(Row)
	rej_df = df.Filter(
		dataframe.F{"REJECTION_TIMESTAMP", series.Neq, ' '},
	)

}
