package inmemory

import (
	"github.com/jbvmio/team"
	"go.uber.org/zap"
)

func addIndex(r team.TaskRequest) {
	request := r.(*Request)
	_, ok := indexes[request.Index]
	if ok {
		Logger.Warn("Index Exists")
		return
	}
	Logger.Debug("Adding Index")
	indexes[request.Index] = NewIndex()
	return
}

func deleteEntry(r team.TaskRequest) {
	request := r.(*Request)
	db, err := indexes[request.Index].GetDB(request.DB)
	if err != nil {
		Logger.Error("Error Retrieving Database",
			zap.Error(err),
		)
		return
	}
	db.Lock()
	_, err = db.GetEntry(request.Entry)
	if err != nil {
		Logger.Error("Error Retrieving Entry",
			zap.Error(err),
		)
		db.Unlock()
		return
	}

	//delete(*db.EntryMap(), request.Entry)
	db.DeleteEntry(request.Entry)
	db.Unlock()
	Logger.Debug("ok")
}

func fetchEntryList(r team.TaskRequest) {
	request := r.(*Request)
	defer close(request.Reply)
	Logger.Debug("Fetching Entries")

	db, err := indexes[request.Index].GetDB(request.DB)
	if err != nil {
		Logger.Error("Error Retrieving Database",
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

	Logger.Debug("ok")
	request.Reply <- entryList
}

func fetchEntry(r team.TaskRequest) {
	request := r.(*Request)
	defer close(request.Reply)
	Logger.Debug("Fetching Entry")

	db, err := indexes[request.Index].GetDB(request.DB)
	if err != nil {
		Logger.Error("Error Retrieving Database",
			zap.Error(err),
		)
		return
	}

	db.RLock()
	data, err := db.GetEntry(request.Entry)
	if err != nil {
		Logger.Error("Error Retrieving Entry",
			zap.Error(err),
		)
		db.RUnlock()
		return
	}
	db.RUnlock()

	Logger.Debug("ok")
	request.Reply <- data
}

func addEntry(r team.TaskRequest) {
	request := r.(*Request)
	index, ok := indexes[request.Index]
	if !ok {
		if !AutoIndex {
			Logger.Error("unknown index",
				zap.String("index", request.Index),
			)
			return
		}
		Logger.Debug("Auto-Adding Index")
		addIndex(request)
		index = indexes[request.Index]
	}
	Logger.Debug("Adding Data")

	index.Lock()
	db, err := index.GetDB(request.DB)
	if err != nil {
		if err.(Err).Code() == ErrUnknownDB {
			Logger.Debug("Creating New Database")
			db = NewDatabase()
			index.AddDB(request.DB, db)
		} else {
			Logger.Error("Error Retrieving Database",
				zap.Error(err),
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

	indexList := make([]string, 0, len(indexes))
	for i := range indexes {
		indexList = append(indexList, i)
	}
	Logger.Debug("ok")
	request.Reply <- indexList
}
