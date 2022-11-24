package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	// MinInnodbIOCapacity min value of innodb_io_capacity
	MinInnodbIOCapacity = 200
)

// InnodbIOCapacityTuner tuner for innodb_io_capacity param
type InnodbIOCapacityTuner struct {
	name        string
	interval    uint
	ctx         context.Context
	db          *db.DB
	notifySvc   *notify.Service
	sendMessage func(Message)
}

// NewInnodbIOCapacityTuner returns an instance of InnodbIOCapacityTuner
func NewInnodbIOCapacityTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
	sendMessage func(Message),
) *InnodbIOCapacityTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &InnodbIOCapacityTuner{
		name:        "innodb_io_capacity",
		interval:    interval,
		ctx:         ctx,
		db:          db,
		notifySvc:   notifySvc,
		sendMessage: sendMessage,
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
		if globalVariables.InnodbIOCapacity == value {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.InnodbIOCapacity", globalVariables.InnodbIOCapacity),
				zap.Uint64("value", value))
			continue
		}

		t.sendMessage(Message{
			Section: "mysqld",
			Key:     strings.ReplaceAll(t.name, "_", "-"),
			Value:   fmt.Sprintf("%d", value),
		})

		_, err = t.db.Exec("SET GLOBAL innodb_io_capacity = ?", value)
		if err != nil {
			log.Logger().Error("set innodb_io_capacity failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", value))
			continue
		}

		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, globalVariables.InnodbIOCapacity, value, now.String()),
			Time:    now,
		})
	}
}
