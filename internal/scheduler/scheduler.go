package scheduler

import (
	"container-bench/internal/dataframe"
	"container-bench/internal/logging"
	"github.com/sirupsen/logrus"
)

type Scheduler interface {
	Initialize() error
	ProcessDataFrames(dataframes *dataframe.DataFrames) error
	Shutdown() error
	GetVersion() string
	SetLogLevel(level string) error
}

type DefaultScheduler struct {
	name            string
	version         string
	schedulerLogger *logrus.Logger
}

func NewDefaultScheduler() *DefaultScheduler {
	return &DefaultScheduler{
		name:            "default",
		version:         "1.0.0",
		schedulerLogger: logging.GetSchedulerLogger(),
	}
}

func (ds *DefaultScheduler) Initialize() error {
	// Default scheduler doesn't need initialization
	return nil
}

func (ds *DefaultScheduler) ProcessDataFrames(dataframes *dataframe.DataFrames) error {
	containers := dataframes.GetAllContainers()
	
	for containerIndex, containerDF := range containers {
		latest := containerDF.GetLatestStep()
		if latest == nil {
			continue
		}
		
		// Simple and lightweight: just print current CPU and cache miss rate
		if latest.Perf != nil && latest.Perf.CacheMissRate != nil {
			ds.schedulerLogger.WithFields(logrus.Fields{
				"container":        containerIndex,
				"cache_miss_rate": *latest.Perf.CacheMissRate,
			}).Info("Cache miss rate")
		}
		
		if latest.Docker != nil && latest.Docker.CPUUsagePercent != nil {
			ds.schedulerLogger.WithFields(logrus.Fields{
				"container":   containerIndex,
				"cpu_percent": *latest.Docker.CPUUsagePercent,
			}).Info("CPU usage")
		}
	}
	
	return nil
}

func (ds *DefaultScheduler) Shutdown() error {
	// Default scheduler doesn't need cleanup
	return nil
}

func (ds *DefaultScheduler) GetVersion() string {
	return ds.version
}

func (ds *DefaultScheduler) SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	ds.schedulerLogger.SetLevel(logLevel)
	return nil
}
