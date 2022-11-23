package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"mtuned/pkg/util"
	"time"

	"go.uber.org/zap"
)

const (
	// MinInnodbLogBufferSize min value of innodb_log_buffer_size (1MB)
	MinInnodbLogBufferSize = 1 * 1024 * 1024
	// MaxInnodbLogBufferSize max value of innodb_log_buffer_size
	MaxInnodbLogBufferSize = 4294967295
)

// InnodbLogBufferSizeTuner tuner for innodb_log_buffer_size param
type InnodbLogBufferSizeTuner struct {
	name      string
	interval  uint
	ctx       context.Context
	db        *db.DB
	notifySvc *notify.Service
}

// NewInnodbLogBufferSizeTuner returns an instance of InnodbLogBufferSizeTuner
func NewInnodbLogBufferSizeTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
) *InnodbLogBufferSizeTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &InnodbLogBufferSizeTuner{
		name:      "innodb_log_buffer_size",
		interval:  interval,
		ctx:       ctx,
		db:        db,
		notifySvc: notifySvc,
	}
	return tuner
}

// Name returns name of tuned parameter
func (t *InnodbLogBufferSizeTuner) Name() string {
	return t.name
}

// Run runs tuner for max_connections
func (t *InnodbLogBufferSizeTuner) Run() {
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

		if globalVariables.InnodbLogBufferSize != 0 && float64(innodbStatus.Log.LSN-innodbStatus.Log.LogFlushedUpTo)/float64(globalVariables.InnodbLogBufferSize) < 0.75 {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.InnodbLogBufferSize", globalVariables.InnodbLogBufferSize),
				zap.Uint64("innodbStatus.Log.LSN", innodbStatus.Log.LSN),
				zap.Uint64("innodbStatus.Log.LogFlushedUpTo", innodbStatus.Log.LogFlushedUpTo))
			continue
		}

		value := util.NextPowerOfTwo(globalVariables.InnodbLogBufferSize)
		if value < MinInnodbLogBufferSize {
			value = MinInnodbLogBufferSize
		} else if value > MaxInnodbLogBufferSize {
			value = MaxInnodbLogBufferSize
		}

		_, err = t.db.Exec("SET GLOBAL innodb_log_buffer_size = ?", value)
		if err != nil {
			log.Logger().Error("set innodb_log_buffer_size failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", value))
			continue
		}

		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, globalVariables.InnodbLogBufferSize, value, now.String()),
			Time:    now,
		})
	}
}
