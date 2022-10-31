package tuner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mtuned/pkg/config"
	"mtuned/pkg/db"
	"mtuned/pkg/log"
	"os/exec"
	"regexp"
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

var (
	ssdRegexp = regexp.MustCompile(`Rotation Rate:\s*Solid State Device`)
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

type device struct {
	name      string
	zfsVolume *string
}

type service struct {
	ctx           context.Context
	tuners        []Tuner
	storage       uint8
	broadcastChan chan broadcastData
	ioState       ioState
	listenerMap   map[unsafe.Pointer]struct{}
	device        device
	bold          bool
	db            *db.DB
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
	pool := db.NewDB()

	var dataDir struct {
		Path string `db:"@@datadir"`
	}
	err := pool.Get(&dataDir, "SELECT @@datadir;")
	if err != nil {
		return nil, err
	}

	dev, err := detectDevice(dataDir.Path)
	if err != nil {
		return nil, err
	}

	var storage int8
	if cfg.SSD == config.StorageAutoDetect {
		storage, err = detectStorage(dev.name)
		if err != nil {
			log.Logger().Error("can't detect storage type, using default", zap.NamedError("error", err))
			storage = config.StorageSpinning
		}
	} else {
		storage = int8(cfg.SSD)
	}

	hpAlloc, err := hugePageAllocation()
	if err != nil {
		return nil, err
	}

	svc := &Service{
		service: service{
			ctx:         ctx,
			storage:     uint8(storage),
			listenerMap: make(map[unsafe.Pointer]struct{}),
			device:      *dev,
			bold:        cfg.Bold,
			db:          pool,
		},
	}

	svc.tuners = []Tuner{
		NewMaxConnectionsTuner(ctx, pool, cfg.Interval.MaxConnections),
		NewInnodbBufPoolSizeTuner(ctx, pool, cfg.Interval.InnodbBufPoolSize, hpAlloc),
		NewTableOpenCacheTuner(ctx, pool, cfg.Interval.TableOpenCache),
		NewKeyBufferSizeTuner(ctx, pool, cfg.Interval.KeyBufSize),
		NewTableDefinitionCacheTuner(ctx, pool, cfg.Interval.TableDefCache),
		NewInnodbFlushNeighborsTuner(ctx, pool, cfg.Interval.InnodbflushNBR, storage),
		NewInnodbBufPoolInstsTuner(ctx, pool, cfg.Interval.InnodbBufPoolInst),
		NewInnodbIOCapacityMaxTuner(ctx, pool, cfg.Interval.InnodbIOCapMax, svc.listenerRegister),
		NewTableOpenCacheInstsTuner(ctx, pool, cfg.Interval.TableOpenCacheInst),
		NewInnodbIOCapacityTuner(ctx, pool, cfg.Interval.InnodbIOCap),
		NewInnodbLogBufferSizeTuner(ctx, pool, cfg.Interval.InnodbLogBufSize),
		NewInnodbLogFileSizeTuner(ctx, pool, cfg.Interval.InnodbLogFileSize),
	}
	svc.broadcastChan = make(chan broadcastData, len(svc.listenerMap))

	return svc, nil
}

// Run runs all tuners
func (s *Service) Run() {
	go s.inferIOState()
	go s.tuneQueryCache()
	go s.tuneZFS()
	go s.tuneOS()

	for _, tuner := range s.tuners {
		go tuner.Run()
	}

	for {
		ticker := time.NewTicker(2 * time.Duration(DefaultTuneInterval) * time.Second)

		select {
		case <-ticker.C:
		case <-s.ctx.Done():
			return
		}

		go s.inferIOState()
	}
}

func (s *Service) broadcast(data broadcastData) {
	for range s.listenerMap {
		s.broadcastChan <- data
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

func (s *Service) inferIOState() {
	var buffer bytes.Buffer
	iostatCmd := exec.Command("iostat", "-x", "1", s.device.name)
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
		if !strings.HasPrefix(line, s.device.name) {
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
	s.ioState = ist
	s.broadcast(broadcastData{
		name:  broadcastDataIOState,
		value: s.ioState,
	})
}

func (s *Service) listenerRegister(p unsafe.Pointer) func(p unsafe.Pointer) <-chan broadcastData {
	s.listenerMap[p] = struct{}{}
	return s.broadcastChannel
}

func (s *Service) broadcastChannel(p unsafe.Pointer) <-chan broadcastData {
	_, ok := s.listenerMap[p]
	if !ok {
		log.Logger().Warn("Not registered listener trying to get broadcast message")
		return nil
	}

	return s.broadcastChan
}

type lsblkLine struct {
	kname  string
	fstype string
	mp     string
}
type byMountPoint []lsblkLine

func (bmp byMountPoint) Len() int           { return len(bmp) }
func (bmp byMountPoint) Less(i, j int) bool { return len(bmp[i].mp) > len(bmp[j].mp) }
func (bmp byMountPoint) Swap(i, j int)      { bmp[i], bmp[j] = bmp[j], bmp[i] }

func detectDevice(dataDir string) (dev *device, err error) {
	output, err := exec.Command("lsblk", "-d", "-o", "KNAME,FSTYPE,MOUNTPOINT").Output()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return nil, errors.New("no device found")
	}

	lsblkLines := make([]lsblkLine, 0)
	for _, line := range lines[1:] {
		parts := strings.Fields(line)
		if len(parts) < 1 {
			continue
		}

		lsblkLine := lsblkLine{
			kname: parts[0],
		}
		if len(parts) > 1 {
			lsblkLine.fstype = parts[1]
		}
		if len(parts) > 2 {
			lsblkLine.mp = parts[2]
		}
		lsblkLines = append(lsblkLines, lsblkLine)
	}

	if len(lsblkLines) == 0 {
		return nil, errors.New("no valid device found")
	}

	sort.Sort(byMountPoint(lsblkLines))
	var isZFS bool
	for _, line := range lsblkLines {
		if strings.HasPrefix(dataDir, line.mp) {
			dev = &device{
				name: line.kname,
			}
			isZFS = strings.HasPrefix(line.mp, "zfs")
			break
		}
	}

	if dev == nil {
		return nil, errors.New("no device matches datadir")
	}

	if !isZFS {
		return dev, nil
	}

	output, err = exec.Command("zfs", "list", dataDir).Output()
	if err != nil {
		return nil, err
	}

	lines = strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return dev, errors.New("no zfs volume matches datadir")
	}

	parts := strings.Fields(lines[1])
	if len(parts) != 5 {
		return nil, errors.New("invalid zfs list command output")
	}

	zfsVolume := parts[0]
	dev.zfsVolume = &zfsVolume
	return
}

func detectStorage(deviceName string) (storage int8, err error) {
	output, err := exec.Command("smartctl", "-i", fmt.Sprintf("/dev/%s", deviceName)).Output()
	if err != nil {
		return config.StorageUnknown, err
	}

	matched := ssdRegexp.Find(output)
	if matched != nil && len(matched) > 0 {
		storage = config.StorageSSD
	} else {
		storage = config.StorageSpinning
	}

	return storage, nil
}

func (s *Service) tuneQueryCache() {
	if !s.bold {
		return
	}

	rows := make([]db.GlobalRow, 0)

	err := s.db.Select(&rows, "SHOW VARIABLES LIKE 'query_cache_%';")
	if err != nil {
		log.Logger().Error("check if query_cache_size/query_cache_type variables exist failed", zap.NamedError("error", err))
		return
	}

	var queries []string
	for _, row := range rows {
		if row.Name == "query_cache_size" {
			queries = append(queries, "query_cache_size = 0")
		} else if row.Name == "query_cache_type" {
			queries = append(queries, "query_cache_type = 0")
		}
	}
	if len(queries) == 0 {
		return
	}

	_, err = s.db.Exec(fmt.Sprintf("SET GLOBAL %s;", strings.Join(queries, ", ")))
	if err != nil {
		log.Logger().Error("set global query_cache_size/query_cache_type variables failed", zap.NamedError("error", err))
	}
}

func (s *Service) tuneZFS() {
	if s.device.zfsVolume == nil {
		// not on ZFS
		return
	}

	globalVariables, err := s.db.GetGlobalVariables()
	if err != nil {
		log.Logger().Error("get global variables failed", zap.NamedError("error", err))
		return
	}

	err = exec.Command("zfs", "set",
		"atime=off",
		"compression=lz4",
		"logbias=throughput",
		"primarycache=metadata",
		fmt.Sprintf("recordsize=%d", globalVariables.InnodbPageSize),
		*s.device.zfsVolume,
	).Run()
	if err != nil {
		log.Logger().Error("set zfs variables failed", zap.NamedError("error", err))
	}

	// set global variables
	// TODO: update non-dynamic variables
	_, err = s.db.Exec(`
SET GLOBAL
	innodb_checksum_algorithm = 'none',
	innodb_flush_neighbors = 0,
	innodb_log_write_ahead_size = ?;
`, globalVariables.InnodbPageSize)
	if err != nil {
		log.Logger().Error("set global variables failed", zap.NamedError("error", err))
	}
}

func (s *Service) tuneOS() {
	if !s.bold {
		return
	}

	if s.storage == config.StorageSSD {
		err := exec.Command("bash", "-c", fmt.Sprintf("echo none > /sys/block/%s/queue/scheduler", s.device.name)).Run()
		if err != nil {
			log.Logger().Error("set I/O scheduler to none failed", zap.NamedError("error", err))
		}
	}

	err := s.tuneOSTuned()
	if err != nil {
		log.Logger().Error("tuning profile with tuned-adm failed", zap.NamedError("error", err))
	}

	err = exec.Command("bash", "-c", "echo never > /sys/kernel/mm/transparent_hugepage/enabled").Run()
	if err != nil {
		log.Logger().Error("disable transparent_hugepage failed", zap.NamedError("error", err))
	}
}

func (s *Service) tuneOSTuned() error {
	_, err := exec.LookPath("tuned-adm")
	if err != nil {
		execErr, ok := err.(*exec.Error)
		if ok && execErr.Unwrap() == exec.ErrNotFound {
			return nil
		}

		return err
	}

	// tuned-adm is enabled
	output, err := exec.Command("tuned-adm", "active").Output()
	if err != nil {
		return fmt.Errorf("run tuned-adm active failed: %s", err.Error())
	}

	parts := strings.Fields(string(output))
	if !strings.HasPrefix(string(output), "Current active profile:") || len(parts) < 4 {
		return fmt.Errorf("tuned-adm active returns an unexpected output: %s", string(output))
	}

	var isOnCloud bool
	for _, part := range parts[3:] {
		if strings.HasPrefix(part, "oci-") {
			isOnCloud = true
			break
		}
	}

	if isOnCloud {
		return nil
	}

	output, err = exec.Command("tuned-adm", "list").Output()
	if err != nil {
		return fmt.Errorf("run tuned-adm list failed: %s", err.Error())
	}

	var profileExist bool
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "- throughput-performance") {
			profileExist = true
			break
		}
	}

	if !profileExist {
		return nil
	}

	err = exec.Command("tuned-adm", "profile", "throughput-performance").Run()
	if err != nil {
		return fmt.Errorf("run tuned-adm profile throughput-performance failed: %s", err.Error())
	}

	return nil
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
