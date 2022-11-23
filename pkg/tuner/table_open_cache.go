package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/util"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// TableOpenCacheTuner tuner for table_open_cache parameter
type TableOpenCacheTuner struct {
	name     string
	interval uint
	ctx      context.Context
	db       *db.DB
}

// NewTableOpenCacheTuner returns an instance of NewTableOpenCacheTuner
func NewTableOpenCacheTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
) *TableOpenCacheTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &TableOpenCacheTuner{
		name:     "table_open_cache",
		interval: interval,
		ctx:      ctx,
		db:       db,
	}
}

// Name returns name of tuned parameter
func (t *TableOpenCacheTuner) Name() string {
	return t.name
}

// Run runs tuner for table_open_cache
func (t *TableOpenCacheTuner) Run() {
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

		var openTablesRow db.GlobalRow
		err = t.db.Get(&openTablesRow, "SHOW GLOBAL STATUS LIKE 'Open_tables';")
		if err != nil {
			log.Logger().Error("get global variables 'Open_tables' failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		openTables, err := strconv.ParseUint(openTablesRow.Value, 10, 64)
		if err != nil {
			log.Logger().Error("parse global variables 'Open_tables' failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.String("value", openTablesRow.Value))
			continue
		}

		if openTables < uint64(0.75*float64(globalVariables.TableOpenCache)) {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("openTables", openTables),
				zap.Uint64("globalVariables.TableOpenCache", globalVariables.TableOpenCache))
			continue
		}

		tableOpenCache := util.NextUint64Multiple(globalVariables.TableOpenCache, globalVariables.TableOpenCacheInsts)
		_, err = t.db.Exec("SET GLOBAL table_open_cache = ?", tableOpenCache)
		if err != nil {
			log.Logger().Error("set table_open_cache failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", tableOpenCache))
			continue
		}
	}
}
