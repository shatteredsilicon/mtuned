package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"runtime"
	"time"

	"go.uber.org/zap"
)

const (
	// MaxInnodbBufPoolInsts max value of innodb_buffer_pool_instances
	MaxInnodbBufPoolInsts = 64
)

// InnodbBufPoolInstsTuner tuner for innodb_buffer_pool_instances parameter
type InnodbBufPoolInstsTuner struct {
	name      string
	interval  uint
	ctx       context.Context
	value     *uint64
	db        *db.DB
	notifySvc *notify.Service
}

// NewInnodbBufPoolInstsTuner returns an instance of InnodbBufPoolInstsTuner
func NewInnodbBufPoolInstsTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
) *InnodbBufPoolInstsTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &InnodbBufPoolInstsTuner{
		name:      "innodb_buffer_pool_instances",
		interval:  interval,
		ctx:       ctx,
		db:        db,
		notifySvc: notifySvc,
	}
}

// Name returns name of tuned parameter
func (t *InnodbBufPoolInstsTuner) Name() string {
	return t.name
}

// Run runs tuner for innodb_buffer_pool_instances
func (t *InnodbBufPoolInstsTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		globalVariables, err := t.db.GetGlobalVariables()
		if err != nil {
			log.Logger().Error("get global variables failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		size := uint64(runtime.NumCPU())
		if size > MaxInnodbBufPoolInsts {
			size = MaxInnodbBufPoolInsts
		}

		var poolInsts uint64
		if t.value != nil && *t.value != 0 {
			poolInsts = *t.value
		} else {
			poolInsts = globalVariables.InnodbBufPoolInsts
		}

		if poolInsts == size {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.InnodbBufPoolInsts", globalVariables.InnodbBufPoolInsts),
				zap.Uint64("size", size), zap.Uint64p("t.value", t.value))
			continue
		}

		t.value = &size
		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, poolInsts, size, now.String()),
			Time:    now,
		})
	}
}
