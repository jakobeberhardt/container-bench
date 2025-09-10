package logging

import (
	"os"

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func init() {
	logger = logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		DisableColors: false,
	})
	logger.SetLevel(logrus.InfoLevel)
}

// GetLogger returns the global logger instance
func GetLogger() *logrus.Logger {
	return logger
}

// SetLogLevel sets the logging level from a string
func SetLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logger.SetLevel(logLevel)
	return nil
}

// SetFormatter sets the log formatter
func SetFormatter(formatter logrus.Formatter) {
	logger.SetFormatter(formatter)
}
