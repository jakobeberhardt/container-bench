package logging

import (
	"fmt"
	"io"
	stdlog "log"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	logger           *logrus.Logger // SYSTEM
	schedulerLogger  *logrus.Logger // SCHEDULER
	proberLogger     *logrus.Logger // PROBER
	accountantLogger *logrus.Logger // ACCOUNTANT
	mu               sync.Mutex
)

type prefixedFormatter struct {
	name  string
	color string
	inner logrus.Formatter
}

func (f *prefixedFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if f == nil || f.inner == nil {
		return nil, fmt.Errorf("nil formatter")
	}
	// Clone entry to avoid mutating shared state.
	dup := *entry
	// Clone Data so we can normalize fields without mutating shared state.
	if len(entry.Data) > 0 {
		dup.Data = make(logrus.Fields, len(entry.Data))
		for k, v := range entry.Data {
			dup.Data[k] = v
		}
		if v, ok := dup.Data["container_id"]; ok {
			if s, ok := v.(string); ok {
				dup.Data["container_id"] = shortenID(s)
			}
		}
	}
	prefix := fmt.Sprintf("[%s]", f.name)
	if colorsEnabledFor(entry, f.inner) && f.color != "" {
		prefix = f.color + prefix + ansiReset
	}
	dup.Message = fmt.Sprintf("%s %s", prefix, entry.Message)
	return f.inner.Format(&dup)
}

func shortenID(s string) string {
	const n = 7
	if len(s) <= n {
		return s
	}
	return s[:n]
}

const (
	ansiReset  = "\x1b[0m"
	ansiCyan   = "\x1b[36m"
	ansiGreen  = "\x1b[32m"
	ansiMag    = "\x1b[35m"
	ansiYellow = "\x1b[33m"
)

func colorsEnabledFor(entry *logrus.Entry, inner logrus.Formatter) bool {
	tf, ok := inner.(*logrus.TextFormatter)
	if !ok || tf == nil {
		return false
	}
	if tf.DisableColors {
		return false
	}
	if tf.ForceColors {
		return true
	}
	if entry == nil || entry.Logger == nil {
		return false
	}
	return isTerminalWriter(entry.Logger.Out)
}

func isTerminalWriter(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok || f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

type stdLogWriter struct {
	logger *logrus.Logger
	level  logrus.Level
}

func (w *stdLogWriter) Write(p []byte) (n int, err error) {
	if w == nil || w.logger == nil {
		return len(p), nil
	}
	msg := strings.TrimSpace(string(p))
	if msg == "" {
		return len(p), nil
	}
	w.logger.Log(w.level, msg)
	return len(p), nil
}

// RedirectStdLogToSystem sends Go's standard library logger output to the SYSTEM logger.
// Many third-party libraries (including some RDT/resctrl helpers) use stdlib log directly,
// which otherwise bypasses our log-level controls.
func RedirectStdLogToSystem(level logrus.Level) {
	stdlog.SetOutput(&stdLogWriter{logger: logger, level: level})
	stdlog.SetFlags(0)
	stdlog.SetPrefix("")
}

func newComponentLogger(name string, color string, fieldMap logrus.FieldMap) *logrus.Logger {
	l := logrus.New()
	l.SetOutput(os.Stdout)
	inner := &logrus.TextFormatter{
		FullTimestamp: true,
		DisableColors: false,
	}
	if fieldMap != nil {
		inner.FieldMap = fieldMap
	}
	l.SetFormatter(&prefixedFormatter{name: name, color: color, inner: inner})
	l.SetLevel(logrus.InfoLevel)
	return l
}

func init() {
	logger = newComponentLogger("SYSTEM", ansiCyan, nil)
	schedulerLogger = newComponentLogger("SCHEDULER", ansiGreen, logrus.FieldMap{
		logrus.FieldKeyTime:  "time",
		logrus.FieldKeyLevel: "level",
		logrus.FieldKeyMsg:   "scheduler_msg",
	})
	proberLogger = newComponentLogger("PROBER", ansiMag, nil)
	accountantLogger = newComponentLogger("ACCOUNTANT", ansiYellow, nil)
}

func GetLogger() *logrus.Logger {
	return logger
}

func GetSchedulerLogger() *logrus.Logger {
	return schedulerLogger
}

func GetProberLogger() *logrus.Logger {
	return proberLogger
}

func GetAccountantLogger() *logrus.Logger {
	return accountantLogger
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

func SetProberLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	proberLogger.SetLevel(logLevel)
	return nil
}

func SetAccountantLogLevel(level string) error {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	accountantLogger.SetLevel(logLevel)
	return nil
}

func SetFormatter(formatter logrus.Formatter) {
	mu.Lock()
	defer mu.Unlock()
	logger.SetFormatter(&prefixedFormatter{name: "SYSTEM", color: ansiCyan, inner: formatter})
}
