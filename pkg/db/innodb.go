package db

import (
	"errors"
	"fmt"
	"mtuned/pkg/log"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

var (
	// ErrInvalidInnodbStatus invalid innodb status output error
	ErrInvalidInnodbStatus = errors.New("invalid innodb status output")
)

const (
	innodbStatusLSNPrefix  = "Log sequence number"
	innodbStatusLFUTPrefix = "Log flushed up to"
	innodbStatusLCAPrefix  = "Last checkpoint at"
)

// InnodbStatus innodb status structure
type InnodbStatus struct {
	Log InnodbStatusLog
}

// InnodbStatusLog innodb status log structure
type InnodbStatusLog struct {
	LSN              uint64
	LogFlushedUpTo   uint64
	LastCheckpointAt uint64
}

// EngineStatus SHOW ENGINE xxx STATUS command output structure
type EngineStatus struct {
	Type   string `db:"Type"`
	Name   string `db:"Name"`
	Status string `db:"Status"`
}

// GetInnodbSize reads innodb data size from db
func (e *Executor) GetInnodbSize() (size uint64, err error) {
	err = e.Get(&size, `
SELECT SUM(data_length + index_length)
FROM information_schema.tables
WHERE engine = 'InnoDB';	
`)
	return
}

// GetInnodbStatus returns innodb status read from
// command 'SHOW ENGINE INNODB STATUS' output
func (e *Executor) GetInnodbStatus() (status InnodbStatus, err error) {
	var eStatus EngineStatus
	err = e.Get(&eStatus, `
SHOW ENGINE INNODB STATUS;
`)
	if err != nil {
		return
	}

	for _, line := range strings.Split(eStatus.Status, "\n") {
		for _, prefix := range []string{innodbStatusLSNPrefix, innodbStatusLFUTPrefix, innodbStatusLCAPrefix} {
			if !strings.HasPrefix(line, prefix) {
				continue
			}

			parts := strings.Fields(line)
			if len(parts) != len(strings.Fields(prefix))+1 {
				log.Logger().Error(fmt.Sprintf("parsing innodb %s status failed", prefix), zap.String("line", line))
				return status, ErrInvalidInnodbStatus
			}

			value, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
			if err != nil {
				log.Logger().Error(fmt.Sprintf("parsing innodb %s status failed", prefix), zap.String("line", line))
				return status, ErrInvalidInnodbStatus
			}

			switch prefix {
			case innodbStatusLSNPrefix:
				status.Log.LSN = value
			case innodbStatusLFUTPrefix:
				status.Log.LogFlushedUpTo = value
			case innodbStatusLCAPrefix:
				status.Log.LastCheckpointAt = value
			}
		}
	}

	return
}
