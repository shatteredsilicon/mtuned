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
	// MinTableDefinitonCache min value of table_definition_cache
	MinTableDefinitionCache = 400
	// MaxTableDefinitionCache max value of table_definition_cache
	MaxTableDefinitionCache = 524288
)

// TableDefinitionCacheTuner tuner for table_definition_cache parameter
type TableDefinitionCacheTuner struct {
	name      string
	interval  uint
	ctx       context.Context
	db        *db.DB
	notifySvc *notify.Service
}

// NewTableDefinitionCacheTuner returns an instance of TableDefinitionCacheTuner
func NewTableDefinitionCacheTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
) *TableDefinitionCacheTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &TableDefinitionCacheTuner{
		name:      "table_definition_cache",
		interval:  interval,
		ctx:       ctx,
		db:        db,
		notifySvc: notifySvc,
	}
}

// Name returns name of tuned parameter
func (t *TableDefinitionCacheTuner) Name() string {
	return t.name
}

// Run runs tuner for table_definition_cache
func (t *TableDefinitionCacheTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		tableCount := struct {
			Count uint64 `db:"table_count"`
		}{}
		err := t.db.Get(&tableCount, "SELECT COUNT(1) AS table_count FROM information_schema.tables;")
		if err != nil {
			log.Logger().Error("get table_count failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		globalVariables, err := t.db.GetGlobalVariables()
		if err != nil {
			log.Logger().Error("get global variables failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		value := util.NextPowerOfTwo(tableCount.Count)
		if value < MinTableDefinitionCache {
			value = MinTableDefinitionCache
		} else if value > MaxTableDefinitionCache {
			value = MaxTableDefinitionCache
		}

		if value == globalVariables.TableDefinitionCache {
			continue
		}

		_, err = t.db.Exec("SET GLOBAL table_definition_cache = ?", value)
		if err != nil {
			log.Logger().Error("set table_definition_cache failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", value))
			continue
		}

		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, globalVariables.TableDefinitionCache, value, now.String()),
			Time:    now,
		})
	}
}
