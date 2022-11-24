package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"runtime"
	"strings"
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
	name        string
	interval    uint
	ctx         context.Context
	value       *uint64
	db          *db.DB
	notifySvc   *notify.Service
	sendMessage func(Message)
}

// NewTableOpenCacheInstsTuner returns an instance of TableOpenCacheInstsTuner
func NewTableOpenCacheInstsTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
	sendMessage func(Message),
) *TableOpenCacheInstsTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &TableOpenCacheInstsTuner{
		name:        "table_open_cache_instances",
		interval:    interval,
		ctx:         ctx,
		db:          db,
		notifySvc:   notifySvc,
		sendMessage: sendMessage,
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

		var cacheInsts uint64
		if t.value != nil && *t.value != 0 {
			cacheInsts = *t.value
		} else {
			cacheInsts = globalVariables.TableOpenCacheInsts
		}

		if cacheInsts == size {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.TableOpenCacheInsts", globalVariables.TableOpenCacheInsts),
				zap.Uint64p("t.value", t.value))
			continue
		}

		t.sendMessage(Message{
			Section: "mysqld",
			Key:     strings.ReplaceAll(t.name, "_", "-"),
			Value:   fmt.Sprintf("%d", size),
		})

		t.value = &size
		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, cacheInsts, size, now.String()),
			Time:    now,
		})
	}
}
