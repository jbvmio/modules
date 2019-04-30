package inmemory

import (
	"github.com/jbvmio/team"
	"go.uber.org/zap"
)

func addIndex(r team.TaskRequest) {
	request := r.(*Request)
	index := moduleStorage.GetIndex(request.Index)
	if index == nil {
		Logger.Warn("Index Exists")
		return
	}
	Logger.Debug("Adding Index", zap.String("index", request.Index))
	moduleStorage.NewIndex(request.Index)
	return
}

func deleteEntry(r team.TaskRequest) {
	request := r.(*Request)
	db := moduleStorage.Get(request.Index).GetDB(request.DB)
	if db.err != nil {
		Logger.Error("Error Retrieving Database",
			zap.Error(db.err),
		)
		return
	}
	db.Lock()
	entry := db.GetEntry(request.Entry)
	if entry.Err() != nil {
		Logger.Error("Error Retrieving Entry",
			zap.Error(entry.Err()),
		)
		db.Unlock()
		return
	}
	db.DeleteEntry(request.Entry)
	db.Unlock()
	Logger.Debug("ok")
}

func fetchEntryList(r team.TaskRequest) {
	request := r.(*Request)
	defer close(request.Reply)
	Logger.Debug("Fetching Entries")

	db := moduleStorage.Get(request.Index).GetDB(request.DB)
	if db.err != nil {
		Logger.Error("Error Retrieving Database",
			zap.Error(db.err),
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

	Logger.Debug("ok")
	request.Reply <- entryList
}

func fetchEntry(r team.TaskRequest) {
	request := r.(*Request)
	defer func() {
		Logger.Debug("closing reply channel", zap.String("index", request.Index),
			zap.String("database", request.DB),
			zap.String("entry", request.Entry),
		)
		close(request.Reply)
	}()
	//defer close(request.Reply)
	Logger.Debug("Fetching Entry", zap.String("index", request.Index),
		zap.String("database", request.DB),
		zap.String("entry", request.Entry),
	)
	db := moduleStorage.Get(request.Index).GetDB(request.DB)
	if db.err != nil {
		Logger.Error("Error Retrieving Database",
			zap.Error(db.err),
		)
		return
	}

	db.RLock()
	entry := db.GetEntry(request.Entry)
	if entry.Err() != nil {
		Logger.Error("Error Retrieving Entry",
			zap.Error(entry.Err()),
		)
		db.RUnlock()
		return
	}
	db.RUnlock()

	Logger.Debug("ok", zap.String("fetch entry", request.Entry))
	request.Reply <- entry
}

func fetchAllEntries(r team.TaskRequest) {
	request := r.(*Request)
	defer close(request.Reply)
	Logger.Debug("Fetching Entries")

	db := moduleStorage.Get(request.Index).GetDB(request.DB)
	if db.err != nil {
		Logger.Error("Error Retrieving Database",
			zap.Error(db.err),
		)
		return
	}

	db.RLock()
	entries := *db.EntryMap()
	allEntries := make([]Entry, 0, len(entries))
	for entry := range entries {
		allEntries = append(allEntries, entries[entry])
	}
	db.RUnlock()

	Logger.Debug("ok")
	request.Reply <- allEntries
}

func addEntry(r team.TaskRequest) {
	request := r.(*Request)
	index := moduleStorage.GetIndex(request.Index) //indexes[request.Index]
	if index == nil {
		if !AutoIndex {
			Logger.Error("unknown index",
				zap.String("index", request.Index),
			)
			return
		}
		Logger.Debug("Auto-Adding Index", zap.String("index", request.Index))
		index = moduleStorage.NewIndex(request.Index)
	}
	Logger.Debug("Adding Entry", zap.String("index", request.Index),
		zap.String("database", request.DB),
		zap.String("entry", request.Entry),
	)

	index.Lock()
	db := index.GetDB(request.DB)
	if db.err != nil {
		if db.err.(Err).Code() == ErrUnknownDB {
			Logger.Debug("Creating New Database", zap.String("database", request.DB))
			db = NewDatabase()
			index.AddDB(request.DB, db)
		} else {
			Logger.Error("Error Retrieving Database",
				zap.Error(db.err),
			)
			index.Unlock()
			return
		}
	}

	index.Unlock()
	db.Lock()
	defer db.Unlock()
	db.AddEntry(request.Entry, request.Data)
	Logger.Debug("ok")
	return
}

func fetchIndexList(r team.TaskRequest) {
	request := r.(*Request)
	defer close(request.Reply)
	Logger.Debug("Fetching Indexes")
	moduleStorage.idx.RLock()
	indexList := make([]string, 0, len(moduleStorage.indexes))
	for i := range moduleStorage.indexes {
		indexList = append(indexList, i)
	}
	moduleStorage.idx.RUnlock()
	Logger.Debug("ok")
	request.Reply <- indexList
}

func fetchDBList(r team.TaskRequest) {
	request := r.(*Request)
	defer close(request.Reply)
	index := moduleStorage.GetIndex(request.Index) //indexes[request.Index]
	if index == nil {
		Logger.Error("unknown index",
			zap.String("index", request.Index),
		)
		return
	}
	Logger.Debug("Fetching Databases")
	dbList := make([]string, 0, len(index.db))
	index.Lock()
	for i := range index.db {
		dbList = append(dbList, i)
	}
	index.Unlock()
	Logger.Debug("ok")
	request.Reply <- dbList
}
