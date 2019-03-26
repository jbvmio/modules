package inmemory

import (
	"sync"
)

// Index contains a map of Databases.
type Index struct {
	//idx map[string][]*ring.Ring // Future Feature*
	db map[string]*Database

	// This lock is used when modifying indexes.
	idxLock *sync.RWMutex
}

// Database contains a map of Objects.
type Database struct {
	lock       *sync.RWMutex
	entries    map[string]Object
	lastAccess int64
}

// Data holds the storage Object
type Data struct {
	Object
}

// NewIndex returns a new Index.
func NewIndex() *Index {
	return &Index{
		//idx:     make(map[string][]*ring.Ring),
		db:      make(map[string]*Database),
		idxLock: &sync.RWMutex{},
	}
}

// GetDB returns the specifed Database or error or not found.
func (i *Index) GetDB(db string) (*Database, error) {
	database, ok := i.db[db]
	if !ok {
		return nil, Errf(ErrUnknownDB, "%v", db)
	}
	return database, nil
}

// AddDB add an existing Database to Index DatabaseMap.
func (i *Index) AddDB(db string, database *Database) {
	i.db[db] = database
}

// Lock locks the Index.
func (i *Index) Lock() {
	i.idxLock.Lock()
}

// Unlock unlocks the Index.
func (i *Index) Unlock() {
	i.idxLock.Unlock()
}

// NewDatabase returns a new Database.
func NewDatabase() *Database {
	return &Database{
		lock:    &sync.RWMutex{},
		entries: make(map[string]Object),
	}
}

// GetEntry returns the specified Entry from the Database.
func (db *Database) GetEntry(entry string) (Object, error) {
	data, ok := db.entries[entry]
	if !ok {
		return nil, Errf(ErrUnknownEntry, "%v", entry)
	}
	return data, nil
}

// AddEntry returns the specified Entry from the Database.
func (db *Database) AddEntry(entry string, data Object) {
	db.entries[entry] = data
}

// DeleteEntry deletes the specified Entry from the Database.
func (db *Database) DeleteEntry(entry string) {
	delete(db.entries, entry)
}

// EntryMap returns the specified underlying EntryMap for the Database.
func (db *Database) EntryMap() *map[string]Object {
	return &db.entries
}

// Lock locks the Database.
func (db *Database) Lock() {
	db.lock.Lock()
}

// Unlock locks the Database.
func (db *Database) Unlock() {
	db.lock.Unlock()
}

// RLock puts a Read Lock on the Database.
func (db *Database) RLock() {
	db.lock.Lock()
}

// RUnlock removes a Read Lock the Database.
func (db *Database) RUnlock() {
	db.lock.Unlock()
}
