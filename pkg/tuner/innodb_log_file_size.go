package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"mtuned/pkg/util"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	// MinInnodbLogFileSize min value of innodb_log_buffer_size (4 MB)
	MinInnodbLogFileSize = 4 * 1024 * 1024
	// MaxInnodbLogFileTotalSize max value of innodb_log_file_size * innodb_log_files_in_group (512 GB)
	MaxInnodbLogFileTotalSize = 512 * 1024 * 1024 * 1024
)

// InnodbLogFileSizeTuner tuner for innodb_log_file_size param
type InnodbLogFileSizeTuner struct {
	name        string
	interval    uint
	ctx         context.Context
	db          *db.DB
	value       *uint64
	notifySvc   *notify.Service
	sendMessage func(Message)
}

// NewInnodbLogFileSizeTuner returns an instance of InnodbLogFileSizeTuner
func NewInnodbLogFileSizeTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
	sendMessage func(Message),
) *InnodbLogFileSizeTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &InnodbLogFileSizeTuner{
		name:        "innodb_log_file_size",
		interval:    interval,
		ctx:         ctx,
		db:          db,
		notifySvc:   notifySvc,
		sendMessage: sendMessage,
	}
	return tuner
}

// Name returns name of tuned parameter
func (t *InnodbLogFileSizeTuner) Name() string {
	return t.name
}

// Run runs tuner for max_connections
func (t *InnodbLogFileSizeTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		innodbStatus, err := t.db.GetInnodbStatus()
		if err != nil {
			log.Logger().Error("get innodb status failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		globalVariables, err := t.db.GetGlobalVariables()
		if err != nil {
			log.Logger().Error("get global variables failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		var fileSize uint64
		if t.value != nil && *t.value != 0 {
			fileSize = *t.value
		} else {
			fileSize = globalVariables.InnodbLogFileSize
		}

		if fileSize == 0 || float64(innodbStatus.Log.LSN-innodbStatus.Log.LastCheckpointAt)/float64(fileSize*globalVariables.InnodbLogFilesInGroup) < 0.75 {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("fileSize", fileSize),
				zap.Uint64("innodbStatus.Log.LSN", innodbStatus.Log.LSN),
				zap.Uint64("innodbStatus.Log.LastCheckpointAt", innodbStatus.Log.LastCheckpointAt),
				zap.Uint64("globalVariables.InnodbLogFilesInGroup", globalVariables.InnodbLogFilesInGroup))
			continue
		}

		value := util.NextPowerOfTwo(fileSize)
		if value < MinInnodbLogFileSize {
			value = MinInnodbLogFileSize
		} else if value > fileSize*globalVariables.InnodbLogFilesInGroup {
			value = MaxInnodbLogFileTotalSize / globalVariables.InnodbLogFilesInGroup
		}

		t.sendMessage(Message{
			Section: "mysqld",
			Key:     strings.ReplaceAll(t.name, "_", "-"),
			Value:   fmt.Sprintf("%d", value),
		})

		t.value = &value
		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, fileSize, value, now.String()),
			Time:    now,
		})
	}
}
