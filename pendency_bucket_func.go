package main

func pendency_bucket_func(x int) string {
	if x == 0 {
		return "0"
	} else if x >= 1 && x <= 2 {
		return "1-2"
	} else if x >= 3 && x <= 5 {
		return "3-5"
	} else if x >= 6 && x <= 7 {
		return "6-7"
	} else {
		return "8 and above"
	}
}
