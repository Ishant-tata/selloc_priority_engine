package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

const (
	s3_staging_dir string = "s3://aws-athena-query-results-424830402610-ap-south-1/"
	region         string = "ap-south-1"
	access_key     string = "AKIAWF2O5QAZAP7SIDDK"
	secret_key     string = "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"
)

var Rows_serviceable []Row_serviceable

type Row_serviceable struct {
	Id_pincode  string
	Id_sellerid string
	Id_delivery string
	Id_shipment string
	Id_slaveid  string
	Id_priority string
}

func DownloadFromS3Bucket() {

	os.Setenv("AWS_ACCESS_KEY", access_key)
	os.Setenv("AWS_SECRET_KEY", secret_key)

	bucket := "tuldl-prod"
	item := "sellerPincodeServiceability.csv.gz"
	path := "Pincode_Serviceability_Matrix/Logistics Serviceability Data/slavePriorityMatrix"
	file, err := os.Create(filepath.Join(path, item))
	if err != nil {
		fmt.Println("Error in creating file", err)
	}
	defer file.Close()

	sess, _ := session.NewSession(
		&aws.Config{
			Region: aws.String(region), Credentials: credentials.AnonymousCredentials},
	)
	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		fmt.Println("Error in downloading", err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
}
func prepare_spm_data() dataframe.DataFrame {
	file, _ := os.Open("sellerPincodeServiceability.csv")

	/**
	output is like: [600086 100031 HD TSHIP 100031-BEM P009]
	columns heading: [_id.pinCode _id.sellerID _id.deliveryType _id.shipmentType _id.slaveId priority]
	**/
	defer file.Close()
	csvReader := csv.NewReader(file)
	count := 0
	var df dataframe.DataFrame
	for {
		rec, err := csvReader.Read()
		if err == io.EOF || count == 10000000 {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// do something with read line
		//count++
		var r1 Row_serviceable
		r1.Id_pincode = rec[0]
		r1.Id_sellerid = rec[1]
		r1.Id_delivery = rec[2]
		r1.Id_shipment = rec[3]
		r1.Id_slaveid = rec[4]
		r1.Id_priority = rec[5]
		if r1.Id_priority != "NaN" {
			Rows_serviceable = append(Rows_serviceable, r1)
		}
		count++
	}
	df = dataframe.LoadStructs(Rows_serviceable)
	return df
}

/**
Time take to print output is: 1.0294082616666667
Length of file is : 157901647
**/
func replace_priority_with_int(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("Id_priority").Records()
	var sel2 []string
	for i := 0; i < len(sel1); i++ {
		temp := sel1[i]
		temp = temp[3:]
		sel2 = append(sel2, temp)
	}
	df = df.Mutate(
		series.New(sel2, series.String, "Pr"),
	)
	return df
}
func sort_values_spm(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("Id_sellerid").Records()
	sel2 := df.Col("Id_pincode").Records()
	sel3 := df.Col("Id_delivery").Records()
	sel4 := df.Col("Id_shipment").Records()
	sel5 := df.Col("Id_priority").Records()
	var sel []string
	for i := 0; i < len(sel1); i++ {
		temp := sel1[i] + "=" + sel2[i] + "=" + sel3[i] + "=" + sel4[i] + "=" + sel5[i] + "=" + strconv.Itoa(i)
		sel = append(sel, temp)
	}
	sort.Strings(sel)
	var a1 []string
	var a2 []string
	var a3 []string
	var a4 []string
	var a5 []string
	for i := 0; i < len(sel); i++ {
		temp := sel[i]
		temp = temp[len(temp)-1:]
		j, _ := strconv.Atoi(temp)
		a1 = append(a1, sel1[j])
		a2 = append(a2, sel2[j])
		a3 = append(a3, sel3[j])
		a4 = append(a4, sel4[j])
		a5 = append(a5, sel5[j])
	}
	sorted := dataframe.New(
		series.New(a2, series.String, "Id_pincode"),
		series.New(a1, series.String, "Id_sellerid"),
		series.New(a3, series.String, "Id_delivery"),
		series.New(a4, series.String, "Id_shipment"),
		series.New(df.Col("Id_slaveid"), series.String, "Id_slaveid"),
		series.New(a5, series.String, "Id_priority"),
		series.New(df.Col("Pr").Records(), series.String, "Pr"),
	)
	return sorted
}
func unique_seller_id(df dataframe.DataFrame) []string {
	var ans []string
	hm := make(map[string]int, 0)
	sel := df.Col("Id_sellerid").Records()
	for i := 0; i < len(sel); i++ {
		_, check := hm[sel[i]]
		if check == false {
			ans = append(ans, sel[i])
			hm[sel[i]] = i
		}
	}
	return ans
}
func drop_duplicates_seller_id_slave_id(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("Id_sellerid").Records()
	sel2 := df.Col("Id_slaveid").Records()
	hm := make(map[string]int, 0)
	var sel []string
	for i := 0; i < len(sel1); i++ {
		temp := sel1[i] + "[]" + sel2[i]
		sel = append(sel, temp)
	}
	var sel1_ans []string
	var sel2_ans []string
	for i := 0; i < len(sel); i++ {
		_, check := hm[sel[i]]
		if check == false {
			hm[sel[i]] = i
			sel1_ans = append(sel1_ans, sel1[i])
			sel2_ans = append(sel2_ans, sel2[i])
		}
	}
	var df_ans dataframe.DataFrame
	df_ans = dataframe.New(
		series.New(sel1_ans, series.String, "Id_sellerid"),
		series.New(sel2_ans, series.String, "Id_slaveid"),
	)
	return df_ans
}

func group_by_dddd1(df dataframe.DataFrame) dataframe.DataFrame {
	sel1 := df.Col("Id_sellerid").Records()
	//sel2 := df.Col("Id_slaveid").Records()
	hm := make(map[string]int, 0)
	for i := 0; i < len(sel1); i++ {
		ok, check := hm[sel1[i]]
		if check == true {
			hm[sel1[i]] = ok + 1
		} else {
			hm[sel1[i]] = 1
		}
	}
	var a1 []string
	var a2 []int
	for i := 0; i < len(sel1); i++ {
		ok, _ := hm[sel1[i]]
		if ok != 0 {
			a1 = append(a1, sel1[i])
			a2 = append(a2, ok)
			hm[sel1[i]] = 0
		}
	}
	df_ans := dataframe.New(
		series.New(a1, series.String, "Id_sellerid"),
		series.New(a2, series.String, "Id_slaveid"),
	)
	return df_ans
}
func all_seller_id_have_more_than_one_store(df dataframe.DataFrame) []string {
	var ans []string
	sel1 := df.Col("Id_sellerid").Records()
	sel2 := df.Col("Id_slaveid").Records()
	for i := 0; i < len(sel2); i++ {
		x, _ := strconv.Atoi(sel2[i])
		if x >= 2 {
			ans = append(ans, sel1[i])
		}
	}
	return ans
}
func ispresent(sel []string, ele string) bool {
	for i := 0; i < len(sel); i++ {
		if sel[i] == ele {
			return true
		}
	}
	return false
}
func seller_id_is_in_morethanone(df dataframe.DataFrame, more_than_one []string) dataframe.DataFrame {
	sel1 := df.Col("Id_pincode").Records()
	sel2 := df.Col("Id_sellerid").Records()
	sel3 := df.Col("Id_delivery").Records()
	sel4 := df.Col("Id_shipment").Records()
	sel5 := df.Col("Id_slaveid").Records()
	sel6 := df.Col("Id_priority").Records()
	sel7 := df.Col("Pr").Records()
	var a1 []string
	var a2 []string
	var a3 []string
	var a4 []string
	var a5 []string
	var a6 []string
	var a7 []string
	for i := 0; i < len(sel2); i++ {
		if ispresent(more_than_one, sel2[i]) {
			a1 = append(a1, sel1[i])
			a2 = append(a2, sel2[i])
			a3 = append(a3, sel3[i])
			a4 = append(a4, sel4[i])
			a5 = append(a5, sel5[i])
			a6 = append(a6, sel6[i])
			a7 = append(a7, sel7[i])
		}
	}
	df_ans := dataframe.New(
		series.New(a1, series.String, "Id_pincode"),
		series.New(a2, series.String, "Id_sellerid"),
		series.New(a3, series.String, "Id_delivery"),
		series.New(a4, series.String, "Id_shipment"),
		series.New(a5, series.String, "Id_slaveid"),
		series.New(a6, series.String, "Id_priority"),
		series.New(a7, series.String, "Pr"),
	)
	return df_ans
}
func sellers_are_ship_unique(df dataframe.DataFrame, s string) []string {
	var slave []string
	hm := make(map[string]int, 0)
	sel1 := df.Col("Id_shipment").Records()
	sel2 := df.Col("Id_sellerid").Records()

	for i := 0; i < len(sel1); i++ {
		if sel1[i] == s {
			_, check := hm[sel2[i]]
			if check == false {
				slave = append(slave, sel2[i])
				hm[sel2[i]] = i
			}
		}
	}
	return slave
}
func write_file(lines []string, path string) {
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
}

func grpBySeller(df dataframe.DataFrame, seller_try string) {
	// filter method takes a lot of time.
	// spm_d := df.Filter(
	// 	dataframe.F{
	// 		Colname:    "Id_sellerid",
	// 		Comparator: series.Eq,
	// 		Comparando: seller_try,
	// 	},
	// )
	sel1 := df.Col("Id_pincode").Records()
	sel2 := df.Col("Id_sellerid").Records()
	sel3 := df.Col("Id_delivery").Records()
	sel4 := df.Col("Id_shipment").Records()
	sel5 := df.Col("Id_slaveid").Records()
	sel6 := df.Col("Id_priority").Records()
	sel7 := df.Col("Pr").Records()
	var a1 []string
	var a2 []string
	var a3 []string
	var a4 []string
	var a5 []string
	var a6 []string
	var a7 []string
	for i := 0; i < len(sel2); i++ {
		if sel2[i] == seller_try {
			a1 = append(a1, sel1[i])
			a2 = append(a2, sel2[i])
			a3 = append(a3, sel3[i])
			a4 = append(a4, sel4[i])
			a5 = append(a5, sel5[i])
			a6 = append(a6, sel6[i])
			a7 = append(a7, sel7[i])
		}
	}
	spm_d := dataframe.New(
		series.New(a1, series.String, "Id_pincode"),
		series.New(a2, series.String, "Id_sellerid"),
		series.New(a3, series.String, "Id_delivery"),
		series.New(a4, series.String, "Id_shipment"),
		series.New(a5, series.String, "Id_slaveid"),
		series.New(a6, series.String, "Id_priority"),
		series.New(a7, series.String, "Pr"),
	)
	fmt.Println(spm_d)
}

func main() {
	// Trying to get file from AWS but shows error
	/******************   ERROR
	  	Error in creating file open Pincode_Serviceability_Matrix\Logistics Serviceability Data\slavePriorityMatrix\sellerPincodeServiceability.csv.gz: The system cannot find the path specified.
	  Error in downloading AccessDenied: Access Denied
	          status code: 403, request id: 7VRN8XG6W8JBYTYP, host id: /j1/WTJUZvK5EIHHaLiWvRzdl2pMu6N+FHCcI290p1RPnfIm1ooVTWIP4P7z7GTcOphTF00bKps=
	  panic: runtime error: invalid memory address or nil pointer dereference
	  [signal 0xc0000005 code=0x0 addr=0x0 pc=0x599f4d]

	  goroutine 1 [running]:
	  os.(*File).Name(...)
	          C:/Program Files/Go/src/os/file.go:57
	  main.DownloadFromS3Bucket()
	          C:/Users/ikumar/OneDrive - Tata CLiQ/Desktop/Selloc Priority Engine Project/selloc priority/make_input_for_serviceable_pincodes.go:50 +0x42d
	  main.main()
	          C:/Users/ikumar/OneDrive - Tata CLiQ/Desktop/Selloc Priority Engine Project/selloc priority/make_input_for_serviceable_pincodes.go:53 +0x17
	  exit status 2
	  *******************************/
	//DownloadFromS3Bucket()

	spm := prepare_spm_data() //currently only taking 1Cr. rows and also taking rows only their priority!=NaN
	spm = replace_priority_with_int(spm)

	//spm = sort_values_spm(spm) //INCORRECT- LET'S TRY AGAIN
	//fmt.Println("spm", spm)
	seller_list := unique_seller_id(spm)
	fmt.Println("Length of seller_list:", len(seller_list)) // should be 3534
	dddd1 := drop_duplicates_seller_id_slave_id(spm)

	dddd := group_by_dddd1(dddd1)
	//fmt.Println(dddd) //should be 3534
	more_than_one := all_seller_id_have_more_than_one_store(dddd)
	//fmt.Println(more_than_one)
	fmt.Println("Length of more_than_one is:", len(more_than_one)) // should be 597
	//fmt.Println(spm)
	spm_1 := seller_id_is_in_morethanone(spm, more_than_one)
	fmt.Println("spm_1", spm_1)

	sship_sellers := sellers_are_ship_unique(spm_1, "SSHIP")
	tship_sellers := sellers_are_ship_unique(spm_1, "TSHIP")
	//fmt.Println(sship_sellers)
	//fmt.Println(tship_sellers)
	write_file(sship_sellers, "sship_sellers.txt")
	write_file(tship_sellers, "tship_sellers.txt")

	//spm_df := grpBySeller(spm_1, more_than_one[0])
	// for i := 1; i < len(more_than_one); i++ {
	// 	df1:=grpBySeller(spm_1, more_than_one[i])
	// 	//concat_dataframe(spm_df, df1)
	// 	fmt.Println(df1)
	//

	grpBySeller(spm_1, more_than_one[0])

}
