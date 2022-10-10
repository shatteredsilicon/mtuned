package config

// Tune persistent tune structure
type Tune struct {
	MySQLD Parameter `ini:"mysqld"`
}
