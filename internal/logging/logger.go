package logging

import (
	"os"
	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger
var schedulerLogger *logrus.Logger

func init() {
	logger = logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		DisableColors: false,
	})
	logger.SetLevel(logrus.InfoLevel)
	
	// Initialize scheduler logger with distinct formatting
	schedulerLogger = logrus.New()
	schedulerLogger.SetOutput(os.Stdout)
	schedulerLogger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		DisableColors: false,
		// Add prefix to distinguish scheduler logs
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "time",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "scheduler_msg",
		},
	})
	schedulerLogger.SetLevel(logrus.InfoLevel)
}

func GetLogger() *logrus.Logger {
	return logger
}

func GetSchedulerLogger() *logrus.Logger {
	return schedulerLogger
}

func SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logger.SetLevel(logLevel)
	return nil
}

func SetSchedulerLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	schedulerLogger.SetLevel(logLevel)
	return nil
}

// SetFormatter sets the log formatter
func SetFormatter(formatter logrus.Formatter) {
	logger.SetFormatter(formatter)
}
