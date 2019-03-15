package inmemory

import (
	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/storage"
	"math/rand"
	"regexp"
	"sync"

	"github.com/OneOfOne/xxhash"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// InMemoryStorage is a storage module that maintains the entire data set in memory in a series of maps. It has a
// configurable number of worker goroutines to service requests, and for requests that are group-specific, the group
// and cluster name are used to hash the request to a consistent worker. This assures that requests for a group are
// processed in order.
type InMemoryStorage struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *coop.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	name        string
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
}

type index struct {
	//idx map[string][]*ring.Ring
	db map[string]*database

	// This lock is used when modifying broker topics or offsets
	idxLock *sync.RWMutex

	// This lock is used when modifying the overall consumer list
	// It does not need to be held for modifying an individual group
	dbLock *sync.RWMutex
}

type database struct {
	// This lock is held when using the individual group, either for read or write
	lock       *sync.RWMutex
	entries    map[string]*Data
	lastAccess int64
}

type Data struct {
	storage.Object
}

// Configure validates the configuration for the module, creates a channel to receive requests on, and sets up the
// storage map. If no expiration time for groups is set, a default value of 7 days is used. If no interval count is
// set, a default of 10 intervals is used. If no worker count is set, a default of 20 workers is used.
func (ims *InMemoryStorage) Configure(name string, configRoot string) {
	ims.Log.Info("configuring")

	ims.name = name

	// Set defaults for configs if needed
	viper.SetDefault(configRoot+".intervals", 10)
	viper.SetDefault(configRoot+".expire-group", 604800)
	viper.SetDefault(configRoot+".workers", 10)
	viper.SetDefault(configRoot+".queue-depth", 1)
	viper.SetDefault(configRoot+".auto-index", true)
	ims.intervals = viper.GetInt(configRoot + ".intervals")
	ims.expireGroup = viper.GetInt64(configRoot + ".expire-group")
	ims.numWorkers = viper.GetInt(configRoot + ".workers")
	ims.minDistance = viper.GetInt64(configRoot + ".min-distance")
	ims.queueDepth = viper.GetInt(configRoot + ".queue-depth")
	ims.autoIndex = viper.GetBool(configRoot + ".auto-index")

	ims.requestChannel = make(chan *storage.StorageRequest, ims.queueDepth)
	ims.workersRunning = sync.WaitGroup{}
	ims.mainRunning = sync.WaitGroup{}
	ims.indexes = make(map[string]index)

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
}

// GetCommunicationChannel returns the RequestChannel that has been setup for this module.
func (ims *InMemoryStorage) GetCommunicationChannel() chan *storage.StorageRequest {
	return ims.requestChannel
}

// Start sets up the rest of the storage map for each configured cluster. It then starts the configured number of
// worker routines to handle requests. Finally, it starts a main loop which will receive requests and hash them to the
// correct worker.
func (ims *InMemoryStorage) Start() error {
	ims.Log.Info("starting")

	for i := range viper.GetStringMap("indexes") {
		ims.
			indexes[i] = index{
			//idx:     make(map[string][]*ring.Ring),
			db:      make(map[string]*database),
			idxLock: &sync.RWMutex{},
			dbLock:  &sync.RWMutex{},
		}
	}

	// Start the appropriate number of workers, with a channel for each
	ims.workers = make([]chan *storage.StorageRequest, ims.numWorkers)
	for i := 0; i < ims.numWorkers; i++ {
		ims.workers[i] = make(chan *storage.StorageRequest, ims.queueDepth)
		ims.workersRunning.Add(1)
		go ims.requestWorker(i, ims.workers[i])
	}

	ims.mainRunning.Add(1)
	go ims.mainLoop()
	return nil
}

// Stop closes the incoming request channel, which will close the main loop. It then closes each of the worker
// channels, to close the workers, and waits for all goroutines to exit before returning.
func (ims *InMemoryStorage) Stop() error {
	ims.Log.Info("stopping")

	close(ims.requestChannel)
	ims.mainRunning.Wait()

	for i := 0; i < ims.numWorkers; i++ {
		close(ims.workers[i])
	}
	ims.workersRunning.Wait()

	return nil
}

func (ims *InMemoryStorage) requestWorker(workerNum int, requestChannel chan *storage.StorageRequest) {
	defer ims.workersRunning.Done()

	// Using a map for the request types avoids a bit of complexity below
	var requestTypeMap = map[storage.StorageRequestConstant]func(*storage.StorageRequest, *zap.Logger){
		storage.StorageSetIndex:       ims.addIndex,
		storage.StorageSetData:        ims.addData,
		storage.StorageClearData:      ims.clearData,
		storage.StorageSetDeleteEntry: ims.deleteEntry,
		storage.StorageFetchEntries:   ims.fetchEntryList,
		storage.StorageFetchEntry:     ims.fetchEntry,
	}

	workerLogger := ims.Log.With(zap.Int("worker", workerNum))
	for r := range requestChannel {
		if requestFunc, ok := requestTypeMap[r.RequestType]; ok {
			requestFunc(r, workerLogger.With(
				zap.String("index", r.Index),
				zap.String("entry", r.Entry),
				zap.String("db", r.DB),
				zap.Int64("timestamp", r.Timestamp),
				zap.String("request", r.RequestType.String())))
		}
	}
}

func (ims *InMemoryStorage) mainLoop() {
	defer ims.mainRunning.Done()

	for r := range ims.requestChannel {
		switch r.RequestType {
		case storage.StorageFetchIndexes, storage.StorageFetchEntries, storage.StorageSetIndex:
			// Send to any worker
			ims.workers[int(rand.Int31n(int32(ims.numWorkers)))] <- r
		case storage.StorageSetDeleteEntry, storage.StorageClearData, storage.StorageSetData, storage.StorageFetchEntry:
			// Hash to a consistent worker
			ims.workers[int(xxhash.ChecksumString64(r.Index+r.DB)%uint64(ims.numWorkers))] <- r
		default:
			ims.Log.Error("unknown storage request type",
				zap.Int("request_type", int(r.RequestType)),
			)
			if r.Reply != nil {
				close(r.Reply)
			}
		}
	}
}

func (ims *InMemoryStorage) deleteEntry(request *storage.StorageRequest, requestLogger *zap.Logger) {
	db, ok := ims.indexes[request.Index].db[request.DB]
	if !ok {
		requestLogger.Warn("unknown index or db")
		return
	}
	db.lock.Lock()
	_, ok = db.entries[request.Entry]
	if !ok {
		requestLogger.Warn("unknown entry")
		db.lock.Unlock()
		return
	}
	delete(db.entries, request.Entry)
	db.lock.Unlock()
	requestLogger.Debug("ok")
}

func (ims *InMemoryStorage) fetchEntryList(request *storage.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	requestLogger.Info("Fetching Entries")

	db, ok := ims.indexes[request.Index].db[request.DB]
	if !ok {
		requestLogger.Warn("unknown index or db")
		return
	}

	db.lock.RLock()
	entryList := make([]string, 0, len(db.entries))
	for entry := range db.entries {
		entryList = append(entryList, entry)
	}
	db.lock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- entryList
}

func (ims *InMemoryStorage) fetchEntry(request *storage.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	requestLogger.Info("Fetching Entry")

	db, ok := ims.indexes[request.Index].db[request.DB]
	if !ok {
		requestLogger.Warn("unknown index or db")
		return
	}

	db.lock.RLock()
	entry, ok := db.entries[request.Entry]
	if !ok {
		requestLogger.Warn("unknown entry")
		db.lock.RUnlock()
		return
	}
	db.lock.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- entry
}

func (ims *InMemoryStorage) addIndex(request *storage.StorageRequest, requestLogger *zap.Logger) {
	_, ok := ims.indexes[request.Index]
	if ok {
		requestLogger.Warn("Index Exists")
		return
	}
	requestLogger.Info("Adding Index")
	ims.indexes[request.Index] = index{
		//idx:     make(map[string][]*ring.Ring),
		db:      make(map[string]*database),
		idxLock: &sync.RWMutex{},
		dbLock:  &sync.RWMutex{},
	}
	return
}

func (ims *InMemoryStorage) addData(request *storage.StorageRequest, requestLogger *zap.Logger) {
	indexMap, ok := ims.indexes[request.Index]
	if !ok {
		if !ims.autoIndex {
			requestLogger.Warn("unknown index")
			return
		}
		ims.addIndex(request, requestLogger)
		indexMap = ims.indexes[request.Index]
	}
	requestLogger.Info("Adding Data")
	// Make the database if it does not yet exist
	indexMap.dbLock.Lock()
	dbMap, ok := indexMap.db[request.DB]
	if !ok {
		indexMap.db[request.DB] = &database{
			lock:    &sync.RWMutex{},
			entries: make(map[string]*Data),
		}
		dbMap = indexMap.db[request.DB]
	}
	indexMap.dbLock.Unlock()

	// For the rest of this, we need the write lock
	dbMap.lock.Lock()
	defer dbMap.lock.Unlock()

	dbMap.entries[request.Entry] = &Data{
		request.Object,
	}
	requestLogger.Debug("ok")
	return
}

func (ims *InMemoryStorage) clearData(request *storage.StorageRequest, requestLogger *zap.Logger) {
	indexMap, ok := ims.indexes[request.Index]
	if !ok {
		// Ignore for indexes that we don't know about - should never happen anyways
		requestLogger.Warn("unknown index")
		return
	}

	// Confirm the DB
	indexMap.dbLock.Lock()
	dbMap, ok := indexMap.db[request.DB]
	if !ok {
		// DB Doesn't Exist
		indexMap.dbLock.Unlock()
		return
	}
	indexMap.dbLock.Unlock()

	// For the rest of this, we need the write lock DB
	dbMap.lock.Lock()
	defer dbMap.lock.Unlock()

	dbMap.entries[request.Entry].ClearData()
	requestLogger.Debug("ok")
}
