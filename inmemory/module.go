package inmemory

import (
	"fmt"
	"os"
	"strings"

	"github.com/jbvmio/team"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Module Variables
var (
	// Main Module Logger
	Logger *zap.Logger
	// mainStorage holds an initialized Datastore
	moduleStorage *Datastore
	// True after first initialization of the indexes
	moduleInit bool
)

// Config contains all the settings for the Module.
type Config struct {
	Name            string
	AutoIndex       bool
	LogLevel        string
	Workers         int
	QueueDepth      int
	MaxReqTime      int
	DiscardTimeouts bool
}

// NewConfig returns a new default Config.
func NewConfig() *Config {
	return &Config{
		Name:            "InMemory",
		AutoIndex:       true,
		LogLevel:        "info",
		Workers:         10,
		QueueDepth:      20,
		MaxReqTime:      1,
		DiscardTimeouts: true,
	}
}

// Module is a storage module that maintains the entire data set in memory in a series of maps.
type Module struct {
	Config       *Config
	Process      *team.Team
	Logger       *zap.Logger
	workerConfig *team.Config
}

// NewModule returns a new Module with defaults.
func NewModule(config *Config) *Module {
	if config == nil {
		config = NewConfig()
	}
	module := Module{
		Process: team.NewTeam(&team.Config{
			Name:           config.Name + "-Process",
			Workers:        config.Workers,
			QueueSize:      config.QueueDepth,
			MaxTimeSecs:    config.MaxReqTime,
			CloseOnTimeout: config.DiscardTimeouts,
		}),
	}
	AutoIndex = config.AutoIndex
	module.Config = config
	moduleStorage = New()
	return &module
}

// Start starts the Module.
func (m *Module) Start() {
	m.Logger = configureLogger(m.Config.LogLevel)
	Logger = m.Logger.With(zap.String("Logger", "Request"))
	m.Process.Logger = m.Logger.With(
		zap.String("Logger", "Worker"),
	)
	m.Logger = m.Logger.With(
		zap.String("Logger", "Storage"),
	)
	zap.ReplaceGlobals(m.Logger)
	for k, v := range requestMap.All {
		m.Process.AddTask(k, v)
	}
	for k, v := range requestMap.Consistent {
		m.Process.AddConsist(k, v)
	}
	m.Process.Start()
}

// Stop starts the Module.
func (m *Module) Stop() {
	m.Process.Stop()
	switch m.Process.Logger.(type) {
	case *zap.Logger:
		m.Process.Logger.(*zap.Logger).Sync()
	}
	Logger.Sync()
	m.Logger.Sync()
}

// AddTask here.
func (m *Module) AddTask(id RequestConstant, requestFunc team.RequestHandleFunc) {
	m.Process.AddTask(int(id), requestFunc)
}

// AddConsistent here.
func (m *Module) AddConsistent(id RequestConstant, requestFunc team.RequestHandleFunc) {
	m.Process.AddConsist(int(id), requestFunc)
}

// SendRequest here.
func (m *Module) SendRequest(request team.TaskRequest) bool {
	return m.Process.Submit(request)
}

func configureLogger(logLevel string) *zap.Logger {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer
	switch strings.ToLower(logLevel) {
	case "none":
		return zap.NewNop()
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
	syncOutput = zapcore.Lock(os.Stdout)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		syncOutput,
		level,
	)
	logger := zap.New(core)
	zap.ReplaceGlobals(logger)
	return logger
}
