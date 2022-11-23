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
	// MaxTableOpenCacheInsts max value of table_open_cache_instances
	MaxTableOpenCacheInsts = 64
	// MinTableOpenCacheInsts min value of table_open_cache_instances
	MinTableOpenCacheInsts = 8
)

// TableOpenCacheInstsTuner tuner for table_open_cache_instances parameter
type TableOpenCacheInstsTuner struct {
	name     string
	interval uint
	ctx      context.Context
	value    *uint64
	db       *db.DB
}

// NewTableOpenCacheInstsTuner returns an instance of TableOpenCacheInstsTuner
func NewTableOpenCacheInstsTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
) *TableOpenCacheInstsTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &TableOpenCacheInstsTuner{
		name:     "table_open_cache_instances",
		interval: interval,
		ctx:      ctx,
		db:       db,
	}
}

// Name returns name of tuned parameter
func (t *TableOpenCacheInstsTuner) Name() string {
	return t.name
}

// Run runs tuner for table_open_cache_instances
func (t *TableOpenCacheInstsTuner) Run() {
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
		if size > MaxTableOpenCacheInsts {
			size = MaxTableOpenCacheInsts
		} else if size < MinTableOpenCacheInsts {
			size = MinTableOpenCacheInsts
		}

		if globalVariables.TableOpenCacheInsts == size || (t.value != nil && *t.value == size) {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.TableOpenCacheInsts", globalVariables.TableOpenCacheInsts),
				zap.Uint64p("t.value", t.value))
			continue
		}

		t.value = &size
	}
}
