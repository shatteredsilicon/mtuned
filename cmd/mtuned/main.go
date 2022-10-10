package main

import (
	"flag"
	"fmt"
	"log"

	"mtuned/pkg/config"
	"mtuned/pkg/db"
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
		log.Panic(err)
	}

	// Init db
	err = db.Init(cfg.ToDBConfig())
	if err != nil {
		log.Panic(err)
	}
}
