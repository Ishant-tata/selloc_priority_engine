package main

// file is completed
import (
	"crypto/tls"
	"fmt"

	gomail "gopkg.in/mail.v2"
)

const (
	from     string = "jprabhu@tataunistore.com"
	host     string = "smtp.gmail.com"
	port     int    = 587
	password string = "Cliq@4321"
)

func send_mail(toList string, subject string, message string) {
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", toList)
	m.SetHeader("Subject", "Gomail test subject")
	m.SetBody("text/plain", message)
	d := gomail.NewDialer(host, port, from, password)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	if err := d.DialAndSend(m); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Mail Sent Successfully")
	}
}
