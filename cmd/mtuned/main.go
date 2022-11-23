package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"mtuned/pkg/config"
	"mtuned/pkg/db"
	logPkg "mtuned/pkg/log"
	"mtuned/pkg/tuner"
	"mtuned/pkg/util"
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

	tunerSvc, err := tuner.NewService(context.Background(), cfg)
	if err != nil {
		log.Panic(err)
	}
	tunerSvc.Run()
}
