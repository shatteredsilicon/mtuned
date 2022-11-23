package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/config"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"time"

	"go.uber.org/zap"
)

// InnodbFlushNeighborsTuner tuner for innodb_flush_neighbors parameter
type InnodbFlushNeighborsTuner struct {
	name     string
	interval uint
	ctx      context.Context
	db       *db.DB
	storage  int8
}

// NewInnodbFlushNeighborsTuner returns an instance of InnodbFlushNeighborsTuner
func NewInnodbFlushNeighborsTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	storage int8,
) *InnodbFlushNeighborsTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &InnodbFlushNeighborsTuner{
		name:     "innodb_flush_neighbors",
		interval: interval,
		ctx:      ctx,
		db:       db,
		storage:  storage,
	}
	return tuner
}

// Name returns name of tuned parameter
func (t *InnodbFlushNeighborsTuner) Name() string {
	return t.name
}

// Run runs tuner for innodb_flush_neighbors
func (t *InnodbFlushNeighborsTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		if t.storage != config.StorageSSD {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Int8("t.storage", t.storage),
				zap.Int8("config.StorageSSD", config.StorageSSD))
			continue
		}

		_, err := t.db.Exec("SET GLOBAL innodb_flush_neighbors = ?", 0)
		if err != nil {
			log.Logger().Error("set innodb_flush_neighbors failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Int("value", 0))
			continue
		}
	}
}
