package coop

import (
	"os"
	"sync"

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
	// information, or to fetch the same information. It is serviced by the storage Module.
	StorageChannel chan *storage.Request

	// Modules contains all loaded Modules
	Modules []Module

	// WG - Controlling sync.WaitGroup
	WG sync.WaitGroup

	loadedModules    map[string]Module
	storageModule    *StorageModule
	quitChannel      chan struct{}
	running          sync.WaitGroup
	hasStorageModule bool
}

// NewApplicationContext returns a new ApplicationContext.
// Be sure to defer Logger.Sync() if not using in conjuction with BeginExisting().
func NewApplicationContext(name string) *ApplicationContext {
	app := ApplicationContext{
		Name: name,
	}
	app.Logger, app.LogLevel = ConfigureLogger()
	//defer app.Logger.Sync()

	app.Logger.Info("Creating Application Context",
		zap.String("name", name),
	)

	// Set up two main channels to use for the evaluator and storage coordinators (only StorageChannel for now).
	//   * Consumers and Clusters send offsets to the storage coordinator to populate all the state information
	//   * The Notifiers send evaluation requests to the evaluator coordinator to check status
	//   * The Evaluators send requests to the storage coordinator for detailed information
	//   * The HTTP server sends requests to both the evaluator and storage coordinators to fulfill API requests
	app.StorageChannel = make(chan *storage.Request)
	app.WG = sync.WaitGroup{}
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

	app.Logger.Info("Configuring Modules For Application Context",
		zap.String("name", app.Name),
	)

	// Configure the Package Modules first, in order
	for _, module := range PackageModules {
		if module != nil {
			app.Logger.Info("Loading Package Module",
				zap.String(module.ModuleDetails()),
			)
			app.Modules = append(app.Modules, module)
		}
	}
	// Configure any outside Modules
	for _, module := range OutsideModules {
		if module != nil {
			app.Logger.Info("Loading Outside Module",
				zap.String(module.ModuleDetails()),
			)
			app.Modules = append(app.Modules, module)
		}
	}

	if len(app.Modules) < 1 {
		app.Logger.Error("No Modules Loaded")
		app.ConfigurationValid = false
		return
	}

	// Init Modules
	app.initModules()

	// Configure the modules in order
	for module := range app.loadedModules {
		app.loadedModules[module].Configure()
		if isStorageModule(app.loadedModules[module]) {
			if !app.hasStorageModule {
				app.Logger.Info("Loading Main Storage Module",
					zap.String(app.loadedModules[module].ModuleDetails()),
				)
				app.hasStorageModule = true
				storage := app.loadedModules[module].(StorageModule)
				app.storageModule = &storage
				go app.StartStorage(app.storageModule)
			} else {
				sm := *app.storageModule
				_, name := sm.ModuleDetails()
				app.loadedModules[module].ModuleLogger().Error("Main Storage Module Already Loaded",
					zap.String("loaded storage", name),
				)
				app.Logger.Error("Multiple Storage Modules Loaded")
				app.ConfigurationValid = false
				return
			}
		}
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

	// Start the coordinators
	for i, module := range app.Modules {
		err := module.Start()
		if err != nil {
			// Reverse our way out, stopping coordinators, then exit
			for j := i - 1; j >= 0; j-- {
				module.Stop()
			}
			return 1
		}
	}

	// Signal everything has started
	app.WG.Done()
	// Wait until we're told to exit
	<-exitChannel
	log.Info("Shutdown triggered")
	StopLoadedModules(app.loadedModules)
	// Exit cleanly
	return 0
}

// StopLoadedModules is a helper func for coordinators to stop a list of modules. Given a map of protocol.Module,
// it calls the Stop func on each one. Any errors that are returned are ignored.
func StopLoadedModules(modules map[string]Module) {
	// Stop all the modules passed in
	for _, module := range modules {
		module.Stop()
	}
}

// StartStorage here.
func (app *ApplicationContext) StartStorage(module *StorageModule) {
	app.running.Add(1)
	defer app.running.Done()

	// We only support 1 module right now, so only send to that module
	var channel chan *storage.Request
	for _, module := range app.Modules {
		channel = module.(StorageModule).GetCommunicationChannel()
	}

	for {
		select {
		case request := <-app.StorageChannel:
			// Yes, this forwarder is silly. However, in the future multiple storage modules could be implemented
			// concurrently. However, that will require implementing a router that properly handles sets and
			// fetches and makes sure only 1 module responds to fetches
			channel <- request
		case <-app.quitChannel:
			return
		}
	}
}

func (app *ApplicationContext) initModules() {
	var tmp []Module
	already := make(map[string]bool, len(app.Modules))
	app.quitChannel = make(chan struct{})
	app.loadedModules = make(map[string]Module, len(app.Modules))
	app.running = sync.WaitGroup{}
	wg := &app.running
	for _, module := range app.Modules {
		class, name := module.ModuleDetails()
		if !already[name] {
			already[name] = true
			module.Init(app.quitChannel, wg)
			module.AssignModuleLogger(app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", getCoordType(module)),
				zap.String("class", class),
				zap.String("name", name)),
			)
			module.ModuleLogger().Info("Initializing Module")
			module.AssignApplicationContext(app)
			app.loadedModules[name] = module
			tmp = append(tmp, module)
		} else {
			app.Logger.Error("Duplicate Module Detected",
				zap.String("name", name),
			)
		}
	}
	app.Modules = tmp
}

func getCoordType(m Module) string {
	switch m.(type) {
	case Module:
		switch {
		case m == m.(StorageModule):
			return "storage"
		default:
			return "generic"
		}
	}
	return "unknown"
}

func isStorageModule(m Module) bool {
	switch m.(type) {
	case Module:
		switch {
		case m == m.(StorageModule):
			return true
		}
	}
	return false
}
