package inmemory

import (
	"math/rand"
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

	requestChannel chan *storage.Request
	workersRunning sync.WaitGroup
	mainRunning    sync.WaitGroup
	indexes        map[string]*Index
	workers        []chan *storage.Request

	quitChannel chan struct{}
	running     *sync.WaitGroup
}

// AssignApplicationContext assigns the underlying ApplicationContext.
func (module *InMemoryModule) AssignApplicationContext(app *coop.ApplicationContext) {
	module.App = app
}

// ModuleDetails returns the Module class and name.
func (module *InMemoryModule) ModuleDetails() (string, string) {
	return moduleClass, moduleName
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

// Configure validates the configuration for the module, creates a channel to receive requests on, and sets up the
// storage map. If no expiration time for groups is set, a default value of 7 days is used. If no interval count is
// set, a default of 10 intervals is used. If no worker count is set, a default of 10 workers is used.
func (module *InMemoryModule) Configure() { //name string, configRoot string) {
	module.Log.Info("configuring inmemory module")
	configRoot := `modules.inmemory`

	/*
		fmt.Println(viper.GetString(configRoot + ".name"))
		fmt.Println(viper.GetString(configRoot + ".class"))
		fmt.Println(viper.GetString(configRoot + ".blah"))
	*/

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

	module.requestChannel = make(chan *storage.Request, module.queueDepth)
	module.workersRunning = sync.WaitGroup{}
	module.mainRunning = sync.WaitGroup{}
	module.indexes = make(map[string]*Index)
}

// Start sets up the rest of the storage map for each configured cluster. It then starts the configured number of
// worker routines to handle requests. Finally, it starts a main loop which will receive requests and hash them to the
// correct worker.
func (module *InMemoryModule) Start() error {
	module.Log.Info("starting")

	for i := range viper.GetStringMap("indexes") {
		module.
			indexes[i] = NewIndex()
	}

	// Start the appropriate number of workers, with a channel for each
	module.workers = make([]chan *storage.Request, module.numWorkers)
	for i := 0; i < module.numWorkers; i++ {
		module.workers[i] = make(chan *storage.Request, module.queueDepth)
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
		case storage.StorageSetDeleteEntry, storage.StorageSetEntry, storage.StorageFetchEntry:
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
func (module *InMemoryModule) GetCommunicationChannel() chan *storage.Request {
	return module.requestChannel
}
