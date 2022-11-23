package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
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
	name     string
	interval uint
	ctx      context.Context
	value    *uint64
	db       *db.DB
}

// NewInnodbBufPoolInstsTuner returns an instance of InnodbBufPoolInstsTuner
func NewInnodbBufPoolInstsTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
) *InnodbBufPoolInstsTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &InnodbBufPoolInstsTuner{
		name:     "innodb_buffer_pool_instances",
		interval: interval,
		ctx:      ctx,
		db:       db,
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

		if globalVariables.InnodbBufPoolInsts == size || (t.value != nil && *t.value == size) {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.InnodbBufPoolInsts", globalVariables.InnodbBufPoolInsts),
				zap.Uint64("size", size), zap.Uint64p("t.value", t.value))
			continue
		}

		t.value = &size
	}
}
