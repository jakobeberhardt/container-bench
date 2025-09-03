package main

import (
	"os"

	"github.com/jakobeberhardt/container-bench/cmd"
	log "github.com/sirupsen/logrus"
)

func main() {
	if err := cmd.Execute(); err != nil {
		log.WithError(err).Fatal("Failed to execute command")
		os.Exit(1)
	}
}
