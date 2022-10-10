package config

import (
	"gopkg.in/ini.v1"
)

// Config configuration file structure of mtuned
type Config struct {
	Username        string    `ini:"username"`
	Password        string    `ini:"password"`
	Socket          string    `ini:"socket"`
	Hostname        string    `ini:"hostname"`
	PersistentTune  string    `ini:"persistent_tune"`
	Bold            bool      `ini:"bold"`
	NotifyFrequency int       `ini:"notify_frequency"`
	Log             string    `ini:"log"`
	LogLevel        int       `ini:"log_level"`
	SSD             int       `ini:"ssd"`
	Interval        Parameter `ini:"interval"`
	Notify          `ini:"notify"`
}

// Load loads configuration from path
func Load(path string) (*Config, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return nil, err
	}

	var c Config
	err = cfg.MapTo(&c)

	return &c, err
}
