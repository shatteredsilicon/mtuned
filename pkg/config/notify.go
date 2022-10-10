package config

const (
	// NotifyFrequencyNever notify frequency - never
	NotifyFrequencyNever = iota
	// NotifyFrequencyDaily notify frequency - daily
	NotifyFrequencyDaily
	// NotifyFrequencyImmediately notify frequency - immediately
	NotifyFrequencyImmediately
)

// Notify notify config
type Notify struct {
	Email          string `ini:"email"`
	SMTPUsername   string `ini:"smtp_username"`
	SMTPPassword   string `ini:"smtp_password"`
	SMTPHost       string `ini:"smtp_host"`
	SMTPPort       uint   `ini:"smtp_port"`
	SlackWebhook   string `ini:"slack_webhook"`
	GenericWebhook string `ini:"generic_webhook"`
}
