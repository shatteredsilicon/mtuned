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
	// KeyBufferUnitSize minimal size that
	// key_buffer_size can be increased or decreased with
	KeyBufferUnitSize = 4096
)

// KeyBufferSizeTuner tuner for key_buffer_size parameter
type KeyBufferSizeTuner struct {
	name        string
	interval    uint
	ctx         context.Context
	db          *db.DB
	notifySvc   *notify.Service
	sendMessage func(Message)
}

// NewKeyBufferSizeTuner returns an instance of KeyBufferSizeTuner
func NewKeyBufferSizeTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
	notifySvc *notify.Service,
	sendMessage func(Message),
) *KeyBufferSizeTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &KeyBufferSizeTuner{
		name:        "key_buffer_size",
		interval:    interval,
		ctx:         ctx,
		db:          db,
		notifySvc:   notifySvc,
		sendMessage: sendMessage,
	}
}

// Name returns name of tuned parameter
func (t *KeyBufferSizeTuner) Name() string {
	return t.name
}

type indexKeySize struct {
	Size uint64 `db:"size"`
}

// Run runs tuner for key_buffer_size
func (t *KeyBufferSizeTuner) Run() {
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

		var size indexKeySize
		err = t.db.Get(&size, "SELECT IFNULL(sum(index_length), 0) AS size FROM information_schema.tables WHERE engine = 'MyISAM';")
		if err != nil {
			log.Logger().Error("get index_length failed", zap.String("tuner", t.name), zap.NamedError("error", err))
			continue
		}

		if size.Size < globalVariables.KeyBufSize {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("size.Size", size.Size),
				zap.Uint64("globalVariables.KeyBufSize", globalVariables.KeyBufSize))
			continue
		}

		keyBufferSize := util.NextUint64Multiple(globalVariables.TableOpenCache, KeyBufferUnitSize)
		if keyBufferSize == globalVariables.KeyBufSize {
			log.Logger().Debug(fmt.Sprintf("%s tuner continued", t.name),
				zap.Uint64("keyBufferSize", keyBufferSize),
				zap.Uint64("globalVariables.KeyBufSize", globalVariables.KeyBufSize))
			continue
		}

		t.sendMessage(Message{
			Section: "mysqld",
			Key:     strings.ReplaceAll(t.name, "_", "-"),
			Value:   fmt.Sprintf("%d", keyBufferSize),
		})

		_, err = t.db.Exec("SET GLOBAL key_buffer_size = ?", keyBufferSize)
		if err != nil {
			log.Logger().Error("sets key_buffer_size failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", keyBufferSize))
			continue
		}

		now := time.Now()
		t.notifySvc.Notify(notify.Message{
			Subject: fmt.Sprintf("%s changed", t.name),
			Content: fmt.Sprintf("%s has been changed from %d to %d at %s", t.name, globalVariables.KeyBufSize, keyBufferSize, now.String()),
			Time:    now,
		})
	}
}
