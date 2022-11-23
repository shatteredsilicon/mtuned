package tuner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mtuned/pkg/config"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"go.uber.org/zap"
)

const (
	// DefaultTuneInterval default interval(seconds) for tuner
	DefaultTuneInterval uint = 60
	iostatRunTime            = 60

	broadcastDataIOState = "ioState"
)

// Tuner tuner for tuned parameter
type Tuner interface {
	Run()
	Name() string
}

type ioState struct {
	maxIOSpeed     float64
	currentIOSpeed float64
}

type service struct {
	ctx           context.Context
	tuners        []Tuner
	storage       uint8
	broadcastChan chan broadcastData
	ioState       ioState
	listenerMap   map[unsafe.Pointer]struct{}
}

type broadcastData struct {
	name  string
	value interface{}
}

// Service service of tuner
type Service struct {
	service
}

// NewService returns a new service of tuner
func NewService(ctx context.Context, cfg *config.Config) (*Service, error) {
	storage := cfg.SSD
	if storage == config.StorageAutoDetect {
		storageCmd := exec.Command("hdparm", "-I", "/dev/sdb")
		grepCmd := exec.Command("grep", "Rotation")

		r, w := io.Pipe()
		storageCmd.Stdout = w
		grepCmd.Stdin = r

		err := storageCmd.Start()
		if err == nil {
			err = grepCmd.Start()
		} else {
			storageCmd = exec.Command("smartctl", "-i", "/dev/sdb")
			err = storageCmd.Start()
			if err == nil {
				err = grepCmd.Start()
			}
		}
		if err != nil {
			return nil, err
		}

		var output bytes.Buffer
		grepCmd.Stdout = &output

		err = storageCmd.Wait()
		if err != nil {
			return nil, err
		}

		err = w.Close()
		if err != nil {
			return nil, err
		}

		err = grepCmd.Wait()
		if err != nil {
			return nil, err
		}

		if strings.Contains(output.String(), "Solid State Device") {
			storage = config.StorageSSD
		}
	}

	hpAlloc, err := hugePageAllocation()
	if err != nil {
		return nil, err
	}

	db := db.NewDB()
	svc := &Service{
		service: service{
			ctx:         ctx,
			storage:     uint8(storage),
			listenerMap: make(map[unsafe.Pointer]struct{}),
		},
	}

	svc.tuners = []Tuner{
		NewMaxConnectionsTuner(ctx, db, cfg.Interval.MaxConnections),
		NewInnodbBufPoolSizeTuner(ctx, db, cfg.Interval.InnodbBufPoolSize, hpAlloc),
		NewTableOpenCacheTuner(ctx, db, cfg.Interval.TableOpenCache),
		NewKeyBufferSizeTuner(ctx, db, cfg.Interval.KeyBufSize),
		NewTableDefinitionCacheTuner(ctx, db, cfg.Interval.TableDefCache),
		NewInnodbFlushNeighborsTuner(ctx, db, cfg.Interval.InnodbflushNBR, int8(storage)),
		NewInnodbBufPoolInstsTuner(ctx, db, cfg.Interval.InnodbBufPoolInst),
		NewInnodbIOCapacityMaxTuner(ctx, db, cfg.Interval.InnodbIOCapMax, svc.listenerRegister),
		NewTableOpenCacheInstsTuner(ctx, db, cfg.Interval.TableOpenCacheInst),
		NewInnodbIOCapacityTuner(ctx, db, cfg.Interval.InnodbIOCap),
		NewInnodbLogBufferSizeTuner(ctx, db, cfg.Interval.InnodbLogBufSize),
		NewInnodbLogFileSizeTuner(ctx, db, cfg.Interval.InnodbLogFileSize),
	}
	svc.broadcastChan = make(chan broadcastData, len(svc.listenerMap))

	return svc, nil
}

// Run runs all tuners
func (ts *Service) Run() {
	for _, tuner := range ts.tuners {
		go tuner.Run()
	}

	go ts.inferIOState()
	ticker := time.NewTicker(time.Duration(DefaultTuneInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
		case <-ts.ctx.Done():
			return
		}

		go ts.inferIOState()
	}
}

func (ts *Service) broadcast(data broadcastData) {
	for range ts.listenerMap {
		ts.broadcastChan <- data
	}
}

type iostat struct {
	rs   float64
	ws   float64
	util float64
}

type byUtilDesc []iostat

func (bu byUtilDesc) Len() int           { return len(bu) }
func (bu byUtilDesc) Less(i, j int) bool { return bu[i].util > bu[j].util }
func (bu byUtilDesc) Swap(i, j int)      { bu[i], bu[j] = bu[j], bu[i] }

func (ts *Service) inferIOState() {
	var buffer bytes.Buffer
	iostatCmd := exec.Command("iostat", "-x", "1", "sdb")
	iostatCmd.Stdout = &buffer
	iostatCmd.Stderr = &buffer

	err := iostatCmd.Start()
	if err != nil {
		log.Logger().Error("Start iostat command failed", zap.NamedError("error", err))
		return
	}

	// get output of iostat command
	waitChan := make(chan error)
	go func() {
		var err error

		defer func() {
			waitChan <- err
		}()

		err = iostatCmd.Wait()
	}()

	select {
	case <-time.After(iostatRunTime * time.Second):
		err = iostatCmd.Process.Kill()
		if err != nil {
			log.Logger().Warn(fmt.Sprintf("killing iostate process failed after %d seconds times out", iostatRunTime), zap.NamedError("error", err))
		}
	case err = <-waitChan:
		log.Logger().Error(fmt.Sprintf("iostate command exit before %d seconds times out", iostatRunTime), zap.NamedError("error", err))
	}
	if err != nil {
		return
	}

	// parsing iostat line by line
	iostats := make([]iostat, 0)
	for _, line := range strings.Split(buffer.String(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "sdb") {
			continue
		}

		parts := strings.Split(line, " ")
		if len(parts) != 16 {
			continue
		}

		rs, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Logger().Warn("parsing iostat command output 'r/s' failed", zap.NamedError("error", err), zap.String("line", line))
			continue
		}

		ws, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Logger().Warn("parsing iostat command output 'w/s' failed", zap.NamedError("error", err), zap.String("line", line))
			continue
		}

		ut, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Logger().Warn("parsing iostat command output '%util' failed", zap.NamedError("error", err), zap.String("line", line))
			continue
		}

		iostats = append(iostats, iostat{
			rs:   rs,
			ws:   ws,
			util: ut,
		})
	}

	if len(iostats) == 0 {
		return
	}

	ist := ioState{
		currentIOSpeed: iostats[len(iostats)-1].rs + iostats[len(iostats)-1].ws,
	}

	// infer maximum I/O speed
	sort.Sort(byUtilDesc(iostats))
	var totalSpeed, totalUtil float64
	for i := 0; i < len(iostats) && i < 10; i++ {
		totalSpeed += iostats[i].rs + iostats[i].ws
		totalUtil += iostats[i].util
	}
	if totalUtil == 0 || totalSpeed == 0 {
		return
	}

	ist.maxIOSpeed = totalSpeed / totalUtil * 100
	ts.ioState = ist
	ts.broadcast(broadcastData{
		name:  broadcastDataIOState,
		value: ts.ioState,
	})
}

func (ts *Service) listenerRegister(p unsafe.Pointer) func(p unsafe.Pointer) <-chan broadcastData {
	ts.listenerMap[p] = struct{}{}
	return ts.broadcastChannel
}

func (ts *Service) broadcastChannel(p unsafe.Pointer) <-chan broadcastData {
	_, ok := ts.listenerMap[p]
	if !ok {
		log.Logger().Warn("Not registered listener trying to get broadcast message")
		return nil
	}

	return ts.broadcastChan
}

func hugePageAllocation() (uint64, error) {
	hpTotalOut, err := exec.Command("grep", "-i", "HugePages_Total", "/proc/meminfo").Output()
	if err != nil {
		return 0, err
	}

	hpTotalLines := strings.Split(string(hpTotalOut), "\n")
	if len(hpTotalLines) == 0 {
		return 0, nil
	}

	hpTotalFields := strings.Fields(hpTotalLines[0])
	if len(hpTotalFields) < 2 {
		return 0, nil
	}

	hpTotal, err := strconv.ParseUint(hpTotalFields[1], 10, 64)
	if err != nil {
		return 0, err
	}

	hpSizeOut, err := exec.Command("grep", "-i", "Hugepagesize", "/proc/meminfo").Output()
	if err != nil {
		return 0, err
	}

	hpSizeLines := strings.Split(string(hpSizeOut), "\n")
	if len(hpSizeLines) == 0 {
		return 0, nil
	}

	hpSizeFields := strings.Fields(hpSizeLines[0])
	if len(hpSizeFields) < 3 {
		return 0, nil
	}

	hpSize, err := strconv.ParseUint(hpSizeFields[1], 10, 64)
	if err != nil {
		return 0, err
	}

	return hpSize * 1024 * hpTotal, nil
}
