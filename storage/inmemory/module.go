package inmemory

import (
	"errors"
	"sync"

	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/storage"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Module (storage) is responsible storing desired data.
// watches. It must accept and respond to all StorageRequest types. This interface conforms to the overall
// Module interface, but it adds a func to fetch the channel that the module is listening on for requests, so
// that requests can be forwarded to it by the coordinator.
type Module interface {
	GetCommunicationChannel() chan *storage.StorageRequest
	Configure(string, string)
	Start() error
	Stop() error
}

// Coordinator (storage) manages a single storage module (only one module is supported at this time), making sure it
// is configured, started, and stopped at the appropriate time. It is also responsible for listening to the
// StorageChannel that is provided in the application context and forwarding those requests to the storage module. If
// no storage module has been configured explicitly, the coordinator starts the inmemory module as a default.
type Coordinator struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *coop.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	quitChannel chan struct{}
	modules     map[string]Module
	running     sync.WaitGroup
}

// getModuleForClass returns the correct module based on the passed className. As part of the Configure steps, if there
// is any error, it will panic with an appropriate message describing the problem.
func getModuleForClass(app *coop.ApplicationContext, moduleName string, className string) Module {
	switch className {
	case "inmemory":
		return &InMemoryStorage{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "storage"),
				zap.String("class", className),
				zap.String("name", moduleName),
			),
		}
	default:
		panic("Unknown storage className provided: " + className)
	}
}

// Configure is called to create the configured storage module and call its Configure func to validate the
// configuration and set it up. The coordinator will panic is more than one module is configured, and if no modules have
// been configured, it will set up a default inmemory storage module. If there are any problems, it is expected that
// this func will panic with a descriptive error message, as configuration failures are not recoverable errors.
func (sc *Coordinator) Configure() {
	sc.Log.Info("configuring")
	sc.quitChannel = make(chan struct{})
	sc.modules = make(map[string]Module)
	sc.running = sync.WaitGroup{}

	modules := viper.GetStringMap("storage")
	switch len(modules) {
	case 0:
		// Create a default module
		viper.Set("storage.default.class-name", "inmemory")
		modules = viper.GetStringMap("storage")
	case 1:
		// Have one module. Just continue
		break
	default:
		panic("Only one storage module must be configured")
	}

	// Create all configured storage modules, add to list of storage
	for name := range modules {
		configRoot := "storage." + name
		module := getModuleForClass(sc.App, name, viper.GetString(configRoot+".class-name"))
		module.Configure(name, configRoot)
		sc.modules[name] = module
	}
}

// Start calls the storage module's underlying Start func. If the module Start returns an error, this func stops
// immediately and returns that error to the caller.
//
// We also start a request forwarder goroutine. This listens to the StorageChannel that is provided in the application
// context that all modules receive, and forwards those requests to the storage modules. At the present time, the
// storage subsystem only supports one module, so this is a simple "accept and forward".
func (sc *Coordinator) Start() error {
	sc.Log.Info("starting")

	// Start Storage modules
	err := StartCoordinatorModules(sc.modules)
	if err != nil {
		return errors.New("Error starting storage module: " + err.Error())
	}

	// Start request forwarder
	go sc.mainLoop()
	return nil
}

// Stop calls the configured storage module's underlying Stop func. It is expected that the module Stop will not return
// until the module has been completely stopped. While an error can be returned, this func always returns no error, as
// a failure during stopping is not a critical failure
func (sc *Coordinator) Stop() error {
	sc.Log.Info("stopping")

	close(sc.quitChannel)
	sc.running.Wait()

	// The individual storage modules can choose whether or not to implement a wait in the Stop routine
	StopCoordinatorModules(sc.modules)
	return nil
}

func (sc *Coordinator) mainLoop() {
	sc.running.Add(1)
	defer sc.running.Done()

	// We only support 1 module right now, so only send to that module
	var channel chan *storage.StorageRequest
	for _, module := range sc.modules {
		channel = module.(Module).GetCommunicationChannel()
	}

	for {
		select {
		case request := <-sc.App.StorageChannel:
			// Yes, this forwarder is silly. However, in the future we want to support multiple storage modules
			// concurrently. However, that will require implementing a router that properly handles sets and
			// fetches and makes sure only 1 module responds to fetches
			channel <- request
		case <-sc.quitChannel:
			return
		}
	}
}

// StartCoordinatorModules is a helper func for coordinators to start a list of modules. Given a map of protocol.Module,
// it calls the Start func on each one. If any module returns an error, it immediately stops and returns that error
func StartCoordinatorModules(modules map[string]Module) error {
	// Start all the modules, returning an error if any fail to start
	for _, module := range modules {
		err := module.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

// StopCoordinatorModules is a helper func for coordinators to stop a list of modules. Given a map of protocol.Module,
// it calls the Stop func on each one. Any errors that are returned are ignored.
func StopCoordinatorModules(modules map[string]Module) {
	// Stop all the modules passed in
	for _, module := range modules {
		module.Stop()
	}
}
