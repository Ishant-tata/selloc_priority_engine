package main

import (
	"fmt"

	"github.com/umahmood/haversine"
)

func dist(lat1 float64, lat2 float64, long1 float64, long2 float64) {
	city1 := haversine.Coord{Lat: lat1, Lon: long1}
	city2 := haversine.Coord{Lat: lat2, Lon: long2}
	_, km := haversine.Distance(city1, city2)
	fmt.Println("Distance is: ", km)
}

// for sample data dist(51.45, 45.04, 1.15, 7.42)
// python result is: 850.2092855296023
// Golang library result is: 849.9424694977387
