package notify

import (
	"fmt"
	"net/smtp"
)

const (
	// DefaultSMTPPort default port of SMTP
	DefaultSMTPPort = 587
)

type email struct {
	username string
	password string
	host     string
	port     uint
	sendTo   string
}

func emailSender(sendTo, username, password, host string, port uint) *email {
	e := email{
		username: username,
		password: password,
		host:     host,
		port:     port,
		sendTo:   sendTo,
	}
	if e.port == 0 {
		e.port = DefaultSMTPPort
	}
	return &e
}

// send sends email to linux user
func (e *email) send(subject, content string) (err error) {
	auth := smtp.PlainAuth("", e.username, e.password, e.host)
	return smtp.SendMail(
		fmt.Sprintf("%s:%d", e.host, e.port),
		auth,
		e.username,
		[]string{e.sendTo},
		[]byte(fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n", e.username, e.sendTo, subject, content)),
	)
}
