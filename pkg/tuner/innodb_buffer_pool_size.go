package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/notify"
	"mtuned/pkg/util"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// InnodbBufPoolSizeTuner tuner for innodb_buffer_pool_size parameter
type InnodbBufPoolSizeTuner struct {
	name          string
	interval      uint
	ctx           context.Context
	db            *db.DB
	hugePageAlloc uint64
	notifySvc     *notify.Service
	sendMessage   func(Message)
}

// NewInnodbBufPoolSizeTuner returns an instance of InnodbBufPoolSizeTuner
func NewInnodbBufPoolSizeTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	hugePageAlloc uint64,
	notifySvc *notify.Service,
	sendMessage func(Message),
) *InnodbBufPoolSizeTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &InnodbBufPoolSizeTuner{
		name:          "innodb_buffer_pool_size",
		interval:      interval,
		ctx:           ctx,
		db:            db,
		hugePageAlloc: hugePageAlloc,
		notifySvc:     notifySvc,
		sendMessage:   sendMessage,
	}
}

// Name returns name of tuned parameter
func (t *InnodbBufPoolSizeTuner) Name() string {
	return t.name
}

// Run runs tuner for innodb_buffer_pool_size
func (t *InnodbBufPoolSizeTuner) Run() {
	ticker := time.NewTicker(time.Duration(t.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		}
		log.Logger().Debug(fmt.Sprintf("%s tuner is running", t.name))

		var sysInfo syscall.Sysinfo_t
		err := syscall.Sysinfo(&sysInfo)
		if err != nil {
			log.Logger().Error("get system info failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		globalVariables, err := t.db.GetGlobalVariables()
		if err != nil {
			log.Logger().Error("get global variables failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		innodbSize, err := t.db.GetInnodbSize()
		if err != nil {
			log.Logger().Error("get innodb size failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		expectedRAM := uint64(float64(sysInfo.Totalram)*0.9) * 1024

		var size uint64
		if globalVariables.InnodbBufferPoolSize > innodbSize {
			size = innodbSize
		} else if globalVariables.MaxMemoryUsage() != expectedRAM {
			size = expectedRAM
		}

		size = util.NextUint64Multiple(size, globalVariables.InnodbBufPoolInsts*globalVariables.InnodbBufPoolChunkSize)

		if globalVariables.LargePages && t.hugePageAlloc > 0 && size < t.hugePageAlloc {
			t.notifySvc.Notify(notify.Message{
				Subject: fmt.Sprintf("innodb_buffer_pool_size adjustment warning"),
				Content: fmt.Sprintf("trying to reduce innodb buffer pool below huge page (value = %d, huge page allocation = %d, InnoDB data size = %d)", size, t.hugePageAlloc, innodbSize),
				Time:    time.Now(),
			})
			continue
		}

		if size == globalVariables.InnodbBufferPoolSize {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("globalVariables.InnodbBufferPoolSize", globalVariables.InnodbBufferPoolSize),
				zap.Uint64("size", size))
			continue
		}

		t.sendMessage(Message{
			Section: "mysqld",
			Key:     strings.ReplaceAll(t.name, "_", "-"),
			Value:   util.Uint64ToSizeString(size),
		})

		_, err = t.db.Exec("SET GLOBAL innodb_buffer_pool_size = ?", size)
		if err != nil {
			log.Logger().Error("set innodb_buffer_pool_size failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", size))
			continue
		}

		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, globalVariables.InnodbBufferPoolSize, size, now.String()),
			Time:    now,
		})
	}
}
