package inmemory

import (
	"fmt"

	"github.com/jbvmio/modules/storage"

	"go.uber.org/zap"
)

func (imm *InMemoryModule) requestWorker(workerNum int, requestChannel chan *storage.Request) {
	defer imm.workersRunning.Done()

	// Using a map for the request types avoids a bit of complexity below
	var requestTypeMap = map[storage.RequestConstant]func(*storage.Request, *zap.Logger){
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

func (imm *InMemoryModule) testFunc(request *storage.Request) {
	fmt.Println(request)
}

func (imm *InMemoryModule) deleteEntry(request *storage.Request, requestLogger *zap.Logger) {
	/*
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
	*/

	db, err := imm.indexes[request.Index].GetDB(request.DB)
	if err != nil {
		requestLogger.Error("Error Retrieving Database",
			zap.Error(err),
		)
		return
	}
	db.Lock()
	_, err = db.GetEntry(request.Entry)
	if err != nil {
		requestLogger.Error("Error Retrieving Entry",
			zap.Error(err),
		)
		db.Unlock()
		return
	}

	delete(*db.EntryMap(), request.Entry)
	db.Unlock()
	requestLogger.Debug("ok")
}

func (imm *InMemoryModule) fetchEntryList(request *storage.Request, requestLogger *zap.Logger) {
	defer close(request.Reply)
	requestLogger.Debug("Fetching Entries")

	db, err := imm.indexes[request.Index].GetDB(request.DB)
	if err != nil {
		requestLogger.Error("Error Retrieving Database",
			zap.Error(err),
		)
		return
	}

	db.RLock()
	entries := *db.EntryMap()
	entryList := make([]string, 0, len(entries))
	for entry := range entries {
		entryList = append(entryList, entry)
	}
	db.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- entryList
}

func (imm *InMemoryModule) fetchEntry(request *storage.Request, requestLogger *zap.Logger) {
	defer close(request.Reply)
	requestLogger.Debug("Fetching Entry")

	db, err := imm.indexes[request.Index].GetDB(request.DB)
	if err != nil {
		requestLogger.Error("Error Retrieving Database",
			zap.Error(err),
		)
		return
	}

	db.RLock()
	data, err := db.GetEntry(request.Entry)
	if err != nil {
		requestLogger.Error("Error Retrieving Entry",
			zap.Error(err),
		)
		db.RUnlock()
		return
	}
	db.RUnlock()

	requestLogger.Debug("ok")
	request.Reply <- data
}

func (imm *InMemoryModule) addIndex(request *storage.Request, requestLogger *zap.Logger) {
	_, ok := imm.indexes[request.Index]
	if ok {
		requestLogger.Warn("Index Exists")
		return
	}
	requestLogger.Debug("Adding Index")
	imm.indexes[request.Index] = storage.NewIndex()
	return
}

func (imm *InMemoryModule) addData(request *storage.Request, requestLogger *zap.Logger) {
	index, ok := imm.indexes[request.Index]
	if !ok {
		if !imm.autoIndex {
			requestLogger.Error("unknown index",
				zap.String("index", request.Index),
			)
			return
		}
		requestLogger.Debug("Auto-Adding Index")
		imm.addIndex(request, requestLogger)
		index = imm.indexes[request.Index]
	}
	requestLogger.Debug("Adding Data")

	index.Lock()
	db, err := index.GetDB(request.DB)
	if err != nil {
		if err.(storage.Err).Code() == storage.ErrUnknownDB {
			requestLogger.Debug("Creating New Database")
			db = storage.NewDatabase()
			index.AddDB(request.DB, db)
		} else {
			requestLogger.Error("Error Retrieving Database",
				zap.Error(err),
			)
			index.Unlock()
			return
		}
	}

	index.Unlock()
	db.Lock()
	defer db.Unlock()
	db.AddEntry(request.Entry, &storage.Data{request.Object})

	requestLogger.Debug("ok")
	return
}

func (imm *InMemoryModule) clearData(request *storage.Request, requestLogger *zap.Logger) {
	index, ok := imm.indexes[request.Index]
	if !ok {
		// Ignore for indexes that we don't know about - should never happen anyways
		requestLogger.Error("Error",
			zap.Error(storage.GetErr(storage.ErrUnknownIndex)),
		)
		return
	}

	index.Lock()
	db, err := index.GetDB(request.DB)
	if err != nil {
		requestLogger.Error("Error Retrieving Database",
			zap.Error(err),
		)
		index.Unlock()
		return
	}
	index.Unlock()

	// For the rest of this, we need the write lock DB
	db.Lock()
	defer db.Unlock()

	data, err := db.GetEntry(request.Entry)
	if err != nil {
		requestLogger.Error("Error Retrieving Data",
			zap.Error(err),
		)
	}
	data.ClearData()
	requestLogger.Debug("ok")
}
