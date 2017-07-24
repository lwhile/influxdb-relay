package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"git.gzsunrun.cn/sunruniaas/influxdb-relay/relay"
	"github.com/prometheus/node_exporter/util"
)

var (
	configFile = flag.String("config", "", "Configuration file to use")
)

func startRelay() {
	flag.Parse()

	if *configFile == "" {
		fmt.Fprintln(os.Stderr, "Missing configuration file")
		flag.PrintDefaults()
		os.Exit(1)
	}

	cfg, err := relay.LoadConfigFile(*configFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Problem loading config file:", err)
	}

	r, err := relay.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		r.Stop()
	}()

	log.Println("starting relays...")
	r.Run()
}

func main() {
	util.BackGroundService("influxdb-relay", "influxdb high available relay", nil, startRelay)
}
