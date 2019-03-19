package inmemory

import (
	"math/rand"
	"regexp"
	"sync"

	"github.com/OneOfOne/xxhash"
	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/storage"
	"github.com/spf13/viper"

	"go.uber.org/zap"
)

const (
	moduleName  = `inmemory`
	moduleClass = `inmemory`
)

/*
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

// Module (storage) manages a single storage module (only one module is supported at this time), making sure it
// is configured, started, and stopped at the appropriate time. It is also responsible for listening to the
// StorageChannel that is provided in the application context and forwarding those requests to the storage module. If
// no storage module has been configured explicitly, the coordinator starts the inmemory module as a default.
type Module struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *coop.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	quitChannel chan struct{}
	modules     map[string]Module
	running     sync.WaitGroup
}
*/

// InMemoryModule is a storage module that maintains the entire data set in memory in a series of maps. It has a
// configurable number of worker goroutines to service requests, and for requests that are group-specific, the group
// and cluster name are used to hash the request to a consistent worker. This assures that requests for a group are
// processed in order.
type InMemoryModule struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *coop.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name        string
	class       string
	intervals   int
	numWorkers  int
	expireGroup int64
	minDistance int64
	queueDepth  int
	autoIndex   bool

	requestChannel chan *storage.StorageRequest
	workersRunning sync.WaitGroup
	mainRunning    sync.WaitGroup
	indexes        map[string]index
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp
	workers        []chan *storage.StorageRequest

	quitChannel chan struct{}
	running     *sync.WaitGroup
}

// AssignApplicationContext assigns the underlying ApplicationContext.
func (module *InMemoryModule) AssignApplicationContext(app *coop.ApplicationContext) {
	module.App = app
}

// ModuleDetails returns the Module class and name.
func (module *InMemoryModule) ModuleDetails() (string, string) {
	return module.class, module.name
}

// AssignModuleLogger assigns the underlying ApplicationContext.
func (module *InMemoryModule) AssignModuleLogger(logger *zap.Logger) {
	module.Log = logger
}

// ModuleLogger returns the Modules' underlying Logger.
func (module *InMemoryModule) ModuleLogger() *zap.Logger {
	return module.Log
}

// Init initializes the Module by setting the name, class and
// assigning the passed in channel and waitgroup.
func (module *InMemoryModule) Init(quitChannel chan struct{}, running *sync.WaitGroup) {
	module.name = moduleName
	module.class = moduleClass
	module.quitChannel = quitChannel
	module.running = running
}

// getModuleForClass returns the correct module based on the passed className. As part of the Configure steps, if there
// is any error, it will panic with an appropriate message describing the problem.
/*
func getModuleForClass(app *coop.ApplicationContext, moduleName string, className string) Module {
	switch className {
	case "inmemory":
		return &InMemoryModule{
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
// ** NEEDS TO BE IN COOP PACKAGE **
func (sc *Module) Configure() {
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
*/

// Configure validates the configuration for the module, creates a channel to receive requests on, and sets up the
// storage map. If no expiration time for groups is set, a default value of 7 days is used. If no interval count is
// set, a default of 10 intervals is used. If no worker count is set, a default of 20 workers is used.
func (module *InMemoryModule) Configure() { //name string, configRoot string) {
	module.Log.Info("configuring inmemory module")

	configRoot := `./`
	//module.name = name

	// Set defaults for configs if needed
	viper.SetDefault(configRoot+".intervals", 10)
	viper.SetDefault(configRoot+".expire-group", 604800)
	viper.SetDefault(configRoot+".workers", 10)
	viper.SetDefault(configRoot+".queue-depth", 1)
	viper.SetDefault(configRoot+".auto-index", true)
	module.intervals = viper.GetInt(configRoot + ".intervals")
	module.expireGroup = viper.GetInt64(configRoot + ".expire-group")
	module.numWorkers = viper.GetInt(configRoot + ".workers")
	module.minDistance = viper.GetInt64(configRoot + ".min-distance")
	module.queueDepth = viper.GetInt(configRoot + ".queue-depth")
	module.autoIndex = viper.GetBool(configRoot + ".auto-index")

	module.requestChannel = make(chan *storage.StorageRequest, module.queueDepth)
	module.workersRunning = sync.WaitGroup{}
	module.mainRunning = sync.WaitGroup{}
	module.indexes = make(map[string]index)

	// whitelist/blacklist TODO:
	/*
		whitelist := viper.GetString(configRoot + ".group-whitelist")
		if whitelist != "" {
			re, err := regexp.Compile(whitelist)
			if err != nil {
				module.Log.Panic("Failed to compile group whitelist")
				panic(err)
			}
			module.groupWhitelist = re
		}

		blacklist := viper.GetString(configRoot + ".group-blacklist")
		if blacklist != "" {
			re, err := regexp.Compile(blacklist)
			if err != nil {
				module.Log.Panic("Failed to compile group blacklist")
				panic(err)
			}
			module.groupBlacklist = re
		}
	*/
	module.Log.Info("done")
}

// Start sets up the rest of the storage map for each configured cluster. It then starts the configured number of
// worker routines to handle requests. Finally, it starts a main loop which will receive requests and hash them to the
// correct worker.
func (module *InMemoryModule) Start() error {
	module.Log.Info("starting")

	for i := range viper.GetStringMap("indexes") {
		module.
			indexes[i] = index{
			//idx:     make(map[string][]*ring.Ring),
			db:      make(map[string]*database),
			idxLock: &sync.RWMutex{},
			dbLock:  &sync.RWMutex{},
		}
	}

	// Start the appropriate number of workers, with a channel for each
	module.workers = make([]chan *storage.StorageRequest, module.numWorkers)
	for i := 0; i < module.numWorkers; i++ {
		module.workers[i] = make(chan *storage.StorageRequest, module.queueDepth)
		module.workersRunning.Add(1)
		go module.requestWorker(i, module.workers[i])
	}

	module.mainRunning.Add(1)
	go module.mainLoop()
	return nil
}

// Stop closes the incoming request channel, which will close the main loop. It then closes each of the worker
// channels, to close the workers, and waits for all goroutines to exit before returning.
func (module *InMemoryModule) Stop() error {
	module.Log.Info("stopping")

	close(module.requestChannel)
	module.mainRunning.Wait()

	for i := 0; i < module.numWorkers; i++ {
		close(module.workers[i])
	}
	module.workersRunning.Wait()

	return nil
}

func (module *InMemoryModule) mainLoop() {
	defer module.mainRunning.Done()

	for r := range module.requestChannel {
		switch r.RequestType {
		case storage.StorageFetchIndexes, storage.StorageFetchEntries, storage.StorageSetIndex:
			// Send to any worker
			module.workers[int(rand.Int31n(int32(module.numWorkers)))] <- r
		case storage.StorageSetDeleteEntry, storage.StorageClearData, storage.StorageSetData, storage.StorageFetchEntry:
			// Hash to a consistent worker
			module.workers[int(xxhash.ChecksumString64(r.Index+r.DB)%uint64(module.numWorkers))] <- r
		default:
			module.Log.Error("unknown storage request type",
				zap.Int("request_type", int(r.RequestType)),
			)
			if r.Reply != nil {
				close(r.Reply)
			}
		}
	}
}

// GetCommunicationChannel returns the RequestChannel that has been setup for this module.
func (module *InMemoryModule) GetCommunicationChannel() chan *storage.StorageRequest {
	return module.requestChannel
}

/*
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
*/
