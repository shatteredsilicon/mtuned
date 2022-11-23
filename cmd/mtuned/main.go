package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"mtuned/pkg/config"
	"mtuned/pkg/db"
	logPkg "mtuned/pkg/log"
	"mtuned/pkg/notify"
	"mtuned/pkg/tuner"
	"mtuned/pkg/util"

	"go.uber.org/zap"
)

var (
	showVersion = flag.Bool("version", false, "Print version information.")
	configPath  = flag.String("config", "/etc/mtuned.cnf", "Configuration file path of mtuned.")
)

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println(util.Version)
		return
	}

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Panic("Initialize config package failed: ", err)
	}

	// Init db
	err = db.Init(cfg.ToDBConfig())
	if err != nil {
		log.Panic("Initialize db package failed: ", err)
	}

	err = logPkg.Init(cfg)
	if err != nil {
		log.Panic("Initialize log package failed: ", err)
	}
	defer logPkg.Sync()

	notifySvc := notify.NewService(cfg)
	notifyChan := make(chan interface{})
	runNotify := func() {
		defer func() {
			r := recover()
			if r != nil {
				logPkg.Logger().Error("notify service crashed", zap.Any("recover", r))
			}

			notifyChan <- r
		}()

		notifySvc.Run()
	}
	go runNotify()

	tunerSvc, err := tuner.NewService(context.Background(), cfg, notifySvc)
	if err != nil {
		log.Panic(err)
	}
	tunerChan := make(chan interface{})
	runTuner := func() {
		defer func() {
			r := recover()
			if r != nil {
				logPkg.Logger().Error("tuner service crashed", zap.Any("recover", r))
			}

			tunerChan <- r
		}()

		tunerSvc.Run()
	}
	go runTuner()

	for {
		select {
		case <-notifyChan:
			go runNotify()
		case <-tunerChan:
			go runTuner()
		}
	}
}
