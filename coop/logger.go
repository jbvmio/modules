package coop

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ConfigureLogger returns a configured zap.Logger which can be used for all logging. It also returns a
// zap.AtomicLevel, which can be used to dynamically adjust the level of the logger. The configuration for the logger
// is read from viper, with the following defaults:
//
// logging.level = info
//
// If logging.filename (path to the log file) is provided, a rolling log file is set up using lumberjack. The
// configuration for that log file is read from viper, with the following defaults:
//
// logging.maxsize = 100
// logging.maxbackups = 10
// logging.maxage = 30
// logging.use-localtime = false
// logging.use-compression = false
func ConfigureLogger() (*zap.Logger, *zap.AtomicLevel) {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer

	// Set config defaults for logging
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.maxsize", 100)
	viper.SetDefault("logging.maxbackups", 10)
	viper.SetDefault("logging.maxage", 30)

	// Create an AtomicLevel that we can use elsewhere to dynamically change the logging level
	logLevel := viper.GetString("logging.level")
	switch strings.ToLower(logLevel) {
	case "", "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "panic":
		level = zap.NewAtomicLevelAt(zap.PanicLevel)
	case "fatal":
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		fmt.Printf("Invalid log level supplied. Defaulting to info: %s", logLevel)
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	// If a filename has been set, set up a rotating logger. Otherwise, use Stdout
	logFilename := viper.GetString("logging.filename")
	if logFilename != "" {
		syncOutput = zapcore.AddSync(&lumberjack.Logger{
			Filename:   logFilename,
			MaxSize:    viper.GetInt("logging.maxsize"),
			MaxBackups: viper.GetInt("logging.maxbackups"),
			MaxAge:     viper.GetInt("logging.maxage"),
			LocalTime:  viper.GetBool("logging.use-localtime"),
			Compress:   viper.GetBool("logging.use-compression"),
		})
	} else {
		syncOutput = zapcore.Lock(os.Stdout)
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		syncOutput,
		level,
	)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)
	return logger, &level
}
