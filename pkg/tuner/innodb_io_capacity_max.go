package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"time"
	"unsafe"

	"go.uber.org/zap"
)

const (
	// MinInnodbIOCapacityMax min value of innodb_io_capacity_max
	MinInnodbIOCapacityMax = 2000
)

// InnodbIOCapacityMaxTuner tuner for innodb_io_capacity_max param
type InnodbIOCapacityMaxTuner struct {
	name          string
	interval      uint
	ctx           context.Context
	db            *db.DB
	ioState       ioState
	broadcastChan func(unsafe.Pointer) <-chan broadcastData
}

// NewInnodbIOCapacityMaxTuner returns an instance of InnodbIOCapacityMaxTuner
func NewInnodbIOCapacityMaxTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	listenerRegister func(unsafe.Pointer) func(unsafe.Pointer) <-chan broadcastData,
) *InnodbIOCapacityMaxTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &InnodbIOCapacityMaxTuner{
		name:     "innodb_io_capacity_max",
		interval: interval,
		ctx:      ctx,
		db:       db,
	}
	tuner.broadcastChan = listenerRegister(unsafe.Pointer(tuner))
	return tuner
}

// Name returns name of tuned parameter
func (t *InnodbIOCapacityMaxTuner) Name() string {
	return t.name
}

// Run runs tuner for innodb_io_capacity_max
func (t *InnodbIOCapacityMaxTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case data := <-t.broadcastChan(unsafe.Pointer(t)):
			if data.name == broadcastDataIOState {
				ioSpeed, ok := data.value.(ioState)
				if !ok {
					log.Logger().Warn("get an unexpected storage broadcast data", zap.String("tuner", t.name), zap.Any("value", data.value))
				} else {
					t.ioState = ioSpeed
				}
			}
			continue
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		if t.ioState.currentIOSpeed/t.ioState.maxIOSpeed < 0.75 || t.ioState.maxIOSpeed/t.ioState.currentIOSpeed < 1.2 {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Float64("t.ioState.currentIOSpeed", t.ioState.currentIOSpeed),
				zap.Float64("t.ioState.maxIOSpeed", t.ioState.maxIOSpeed))
			continue
		}

		value := uint64(t.ioState.maxIOSpeed * 0.75)
		if value < MinInnodbIOCapacityMax {
			value = MinInnodbIOCapacityMax
		}

		_, err := t.db.Exec("SET GLOBAL innodb_io_capacity_max = ?", value)
		if err != nil {
			log.Logger().Error("set innodb_flush_neighbors failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", value))
			continue
		}
	}
}
