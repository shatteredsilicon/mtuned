package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"mtuned/pkg/util"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	// MaxMaxConnections max value of max_connections
	MaxMaxConnections = 100000
)

// MaxConnectionsTuner tuner for max_connections param
type MaxConnectionsTuner struct {
	name           string
	interval       uint
	ctx            context.Context
	lastUpdateTime time.Time
	db             *db.DB
	notifySvc      *notify.Service
	sendMessage    func(Message)
}

// NewMaxConnectionsTuner returns an instance of MaxConnectionsTuner
func NewMaxConnectionsTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
	sendMessage func(Message),
) *MaxConnectionsTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	tuner := &MaxConnectionsTuner{
		name:           "max_connections",
		interval:       interval,
		ctx:            ctx,
		db:             db,
		lastUpdateTime: time.Now(),
		notifySvc:      notifySvc,
		sendMessage:    sendMessage,
	}
	return tuner
}

// Name returns name of tuned parameter
func (t *MaxConnectionsTuner) Name() string {
	return t.name
}

// Run runs tuner for max_connections
func (t *MaxConnectionsTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		lastTooManyConnTime := db.LastTooManyConnTime()
		if lastTooManyConnTime == nil || t.lastUpdateTime.After(*lastTooManyConnTime) {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Timep("lastTooManyConnTime", lastTooManyConnTime),
				zap.Time("t.lastUpdateTime", t.lastUpdateTime))
			continue
		}

		globalVariables, err := t.db.GetGlobalVariables()
		if err != nil {
			log.Logger().Error("get global variables failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		value := util.NextPowerOfTwo(globalVariables.MaxConnections)
		if value > MaxMaxConnections {
			value = MaxMaxConnections
		}
		if globalVariables.MaxConnections == value {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.MaxConnections", globalVariables.MaxConnections),
				zap.Uint64("value", value))
			continue
		}

		t.sendMessage(Message{
			Section: "mysqld",
			Key:     strings.ReplaceAll(t.name, "_", "-"),
			Value:   fmt.Sprintf("%d", value),
		})

		_, err = t.db.Exec("SET GLOBAL max_connections = ?", value)
		if err != nil {
			log.Logger().Error("set max_connections failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", value))
			continue
		}

		now := time.Now()
		t.lastUpdateTime = now
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, globalVariables.MaxConnections, value, now.String()),
			Time:    now,
		})
	}
}
