package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"time"

	"go.uber.org/zap"
)

const (
	// MinInnodbIOCapacity min value of innodb_io_capacity
	MinInnodbIOCapacity = 200
)

// InnodbIOCapacityTuner tuner for innodb_io_capacity param
type InnodbIOCapacityTuner struct {
	name     string
	interval uint
	ctx      context.Context
	db       *db.DB
}

// NewInnodbIOCapacityTuner returns an instance of InnodbIOCapacityTuner
func NewInnodbIOCapacityTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
) *InnodbIOCapacityTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &InnodbIOCapacityTuner{
		name:     "innodb_io_capacity",
		interval: interval,
		ctx:      ctx,
		db:       db,
	}
	return tuner
}

// Name returns name of tuned parameter
func (t *InnodbIOCapacityTuner) Name() string {
	return t.name
}

// Run runs tuner for innodb_io_capacity
func (t *InnodbIOCapacityTuner) Run() {
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

		if float64(globalVariables.InnodbIOCapacity)/float64(globalVariables.InnodbIOCapacityMax) == 0.5 {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.InnodbIOCapacity", globalVariables.InnodbIOCapacity),
				zap.Uint64("globalVariables.InnodbIOCapacityMax", globalVariables.InnodbIOCapacityMax))
			continue
		}

		value := uint64(float64(globalVariables.InnodbIOCapacityMax) * float64(0.5))
		if value < MinInnodbIOCapacity {
			value = MinInnodbIOCapacity
		}
		_, err = t.db.Exec("SET GLOBAL innodb_io_capacity = ?", value)
		if err != nil {
			log.Logger().Error("set innodb_io_capacity failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", value))
			continue
		}
	}
}
