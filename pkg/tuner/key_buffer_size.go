package tuner

import (
	"context"
	"fmt"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"mtuned/pkg/util"
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
	name     string
	interval uint
	ctx      context.Context
	db       *db.DB
}

// NewKeyBufferSizeTuner returns an instance of KeyBufferSizeTuner
func NewKeyBufferSizeTuner(
	ctx context.Context,
	db *db.DB,
	interval uint,
) *KeyBufferSizeTuner {
	if interval == 0 {
		interval = DefaultTuneInterval
	}

	return &KeyBufferSizeTuner{
		name:     "key_buffer_size",
		interval: interval,
		ctx:      ctx,
		db:       db,
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
		_, err = t.db.Exec("SET GLOBAL key_buffer_size = ?", keyBufferSize)
		if err != nil {
			log.Logger().Error("sets key_buffer_size failed", zap.String("tuner", t.name), zap.NamedError("error", err), zap.Uint64("value", keyBufferSize))
			continue
		}
	}
}
