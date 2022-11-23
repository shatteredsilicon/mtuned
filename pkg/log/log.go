package log

import (
	"mtuned/pkg/config"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultLogFile default log file path
	DefaultLogFile = "/var/log/mtuned.log"
)

var logger *zap.Logger

// Init initializes log package
func Init(cfg *config.Config) (err error) {
	var logCfg zap.Config
	if zapcore.Level(cfg.LogLevel) > zapcore.DebugLevel {
		logCfg = zap.NewProductionConfig()
	} else {
		logCfg = zap.NewDevelopmentConfig()
	}

	if strings.TrimSpace(cfg.Log) != "" {
		logCfg.OutputPaths = []string{cfg.Log}
	} else {
		logCfg.OutputPaths = []string{DefaultLogFile}
	}

	logger, err = logCfg.Build()
	return
}

// Sync flush buffered log if there is any
func Sync() error {
	return logger.Sync()
}

// Logger returns a copy of zap logger instance
func Logger() *zap.Logger {
	return logger
}
