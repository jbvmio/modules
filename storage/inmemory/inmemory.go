package inmemory

import (
	"sync"

	"github.com/jbvmio/modules/storage"

	"go.uber.org/zap"
)

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

// Data holds the storage Object
type Data struct {
	storage.Object
}

func (imm *InMemoryModule) requestWorker(workerNum int, requestChannel chan *storage.StorageRequest) {
	defer imm.workersRunning.Done()

	// Using a map for the request types avoids a bit of complexity below
	var requestTypeMap = map[storage.StorageRequestConstant]func(*storage.StorageRequest, *zap.Logger){
		storage.StorageSetIndex:       imm.addIndex,
		storage.StorageSetData:        imm.addData,
		storage.StorageClearData:      imm.clearData,
		storage.StorageSetDeleteEntry: imm.deleteEntry,
		storage.StorageFetchEntries:   imm.fetchEntryList,
		storage.StorageFetchEntry:     imm.fetchEntry,
	}

	workerLogger := imm.Log.With(zap.Int("worker", workerNum))
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

func (imm *InMemoryModule) deleteEntry(request *storage.StorageRequest, requestLogger *zap.Logger) {
	db, ok := imm.indexes[request.Index].db[request.DB]
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

func (imm *InMemoryModule) fetchEntryList(request *storage.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	requestLogger.Debug("Fetching Entries")

	db, ok := imm.indexes[request.Index].db[request.DB]
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

func (imm *InMemoryModule) fetchEntry(request *storage.StorageRequest, requestLogger *zap.Logger) {
	defer close(request.Reply)

	requestLogger.Debug("Fetching Entry")

	db, ok := imm.indexes[request.Index].db[request.DB]
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

func (imm *InMemoryModule) addIndex(request *storage.StorageRequest, requestLogger *zap.Logger) {
	_, ok := imm.indexes[request.Index]
	if ok {
		requestLogger.Warn("Index Exists")
		return
	}
	requestLogger.Debug("Adding Index")
	imm.indexes[request.Index] = index{
		//idx:     make(map[string][]*ring.Ring),
		db:      make(map[string]*database),
		idxLock: &sync.RWMutex{},
		dbLock:  &sync.RWMutex{},
	}
	return
}

func (imm *InMemoryModule) addData(request *storage.StorageRequest, requestLogger *zap.Logger) {
	indexMap, ok := imm.indexes[request.Index]
	if !ok {
		if !imm.autoIndex {
			requestLogger.Error("unknown index",
				zap.String("index", request.Index),
			)
			return
		}
		requestLogger.Debug("Auto-Adding Index")
		imm.addIndex(request, requestLogger)
		indexMap = imm.indexes[request.Index]
	}
	requestLogger.Debug("Adding Data")
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

func (imm *InMemoryModule) clearData(request *storage.StorageRequest, requestLogger *zap.Logger) {
	indexMap, ok := imm.indexes[request.Index]
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
