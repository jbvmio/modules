package coop

import (
	"os"

	"github.com/jbvmio/modules/storage"

	"go.uber.org/zap"
)

// ApplicationContext is a structure that holds objects that are used across all coordinators and modules. This is
// used in lieu of passing individual arguments to all functions.
type ApplicationContext struct {
	Name string
	// Logger is a configured zap.Logger instance. It is to be used by the main routine directly, and the main routine
	// creates loggers for each of the coordinators to use that have fields set to identify that coordinator.
	//
	// This field can be set prior to calling core.Start() in order to pre-configure the logger. If it is not set,
	// core.Start() will set up a default logger using the application config.
	Logger *zap.Logger

	// LogLevel is an AtomicLevel instance that has been used to set the default level of the Logger. It is used to
	// dynamically adjust the logging level (such as via an HTTP call)
	//
	// If Logger has been set prior to calling core.Start(), LogLevel must be set as well.
	LogLevel *zap.AtomicLevel

	// This is used by main routines to signal that the configuration is valid. The rest of the code should
	// not care about this, as the application will exit if the configuration is not valid.
	ConfigurationValid bool

	// This is the channel over which any module should send storage requests for storage of offsets and group
	// information, or to fetch the same information. It is serviced by the storage Coordinator.
	StorageChannel chan *storage.StorageRequest

	Modules []Coordinator
}

// NewApplicationContext returns a new ApplicationContext.
// Be sure to defer Logger.Sync() if not using in conjuction with BeginExisting().
func NewApplicationContext(name string) *ApplicationContext {
	app := ApplicationContext{
		Name: name,
	}
	app.Logger, app.LogLevel = ConfigureLogger()
	//defer app.Logger.Sync()

	app.Logger.Info("Creating Application Context")

	// Set up two main channels to use for the evaluator and storage coordinators (only StorageChannel for now).
	//   * Consumers and Clusters send offsets to the storage coordinator to populate all the state information
	//   * The Notifiers send evaluation requests to the evaluator coordinator to check status
	//   * The Evaluators send requests to the storage coordinator for detailed information
	//   * The HTTP server sends requests to both the evaluator and storage coordinators to fulfill API requests
	app.StorageChannel = make(chan *storage.StorageRequest)
	return &app
}

// ConfigureModules configures all the added Modules in the Application Context.
// Run before calling Start.
func (app *ApplicationContext) ConfigureModules() {
	// Configure methods are allowed to panic, as their errors are non-recoverable
	// Catch panics here and flag in the application context if we can't continue
	defer func() {
		if r := recover(); r != nil {
			app.Logger.Panic(r.(string))
			app.ConfigurationValid = false
		}
	}()

	// Configure the Package Modules first, in order
	for _, coordinator := range PackageModules {
		if coordinator != nil {
			app.Modules = append(app.Modules, coordinator)
		}
	}
	// Configure any outside Modules
	for _, coordinator := range OutsideModules {
		if coordinator != nil {
			app.Modules = append(app.Modules, coordinator)
		}
	}

	if len(app.Modules) < 1 {
		app.Logger.Error("No Modules Loaded")
		app.ConfigurationValid = false
		return
	}

	// Configure the coordinators in order
	for _, coordinator := range app.Modules {
		coordinator.Configure()
	}
	app.ConfigurationValid = true
}

// Start the Application Context Modules.
// Returns 1 on any failure, including invalid configurations or a failure to start any modules.
func (app *ApplicationContext) Start(exitChannel chan os.Signal) int {
	// Validate that the ApplicationContext is complete
	if (app == nil) || (app.Logger == nil) || (app.LogLevel == nil) {
		return 1
	}
	defer app.Logger.Sync()
	// Verify Valid Configuration
	if !app.ConfigurationValid {
		return 1
	}
	app.Logger.Info("Starting",
		zap.String("name", app.Name),
	)

	// Set up a specific child logger for main
	log := app.Logger.With(zap.String("type", "main"), zap.String("name", app.Name))

	// Start the coordinators in order
	for i, coordinator := range app.Modules {
		err := coordinator.Start()
		if err != nil {
			// Reverse our way out, stopping coordinators, then exit
			for j := i - 1; j >= 0; j-- {
				coordinator.Stop()
			}
			return 1
		}
	}

	// Wait until we're told to exit
	<-exitChannel
	log.Info("Shutdown triggered")

	// Exit cleanly
	return 0
}
