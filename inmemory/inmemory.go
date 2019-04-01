package inmemory

import (
	"sync"
)

// Global Variables
var (
	// Enable AutoIndexing, Default true
	AutoIndex bool
	// mainStorage holds an initialized Datastore
	defaultStorage *Datastore
	defaultInit    bool
)

// Datastore holds an InMemory storage structure.
type Datastore struct {
	indexes map[string]*Index
	// Mutex for all Indexes
	idx *sync.RWMutex
}

// New creates and returns a new Datastore.
func New() *Datastore {
	return &Datastore{
		indexes: make(map[string]*Index),
		idx:     &sync.RWMutex{},
	}
}

// Get returns the given Index or nil.
func (D *Datastore) Get(index string) *Index {
	D.idx.Lock()
	i := D.indexes[index]
	D.idx.Unlock()
	return i
}

// Entry represents a stored object in a database.
type Entry interface {
	Get() interface{}
	Err() error
}

// Data implements Entry.
type Data struct {
	Item interface{}
	// err contains any errors encountered during the operational process.
	err error
}

// Get returns the internal Data Item.
func (d *Data) Get() interface{} {
	return d.Item
}

// Err returns any errors encountered during the operational process.
func (d *Data) Err() error {
	return d.err
}

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
	entries    map[string]Entry
	lastAccess int64
	// err holds any errors encountered during the operational process.
	err error
}

// NewIndex returns a new Index.
func (D *Datastore) NewIndex(name string) *Index {
	D.idx.Lock()
	i := &Index{
		db:      make(map[string]*Database),
		idxLock: &sync.RWMutex{},
	}
	D.indexes[name] = i
	D.idx.Unlock()
	return i
}

// GetIndex returns the given Index or nil.
func (D *Datastore) GetIndex(name string) *Index {
	return D.Get(name)
}

// NewIndex operates on the defaultDatastore and returns a new Index.
func NewIndex(name string) *Index {
	initDefault()
	defaultStorage.idx.Lock()
	i := &Index{
		db:      make(map[string]*Database),
		idxLock: &sync.RWMutex{},
	}
	defaultStorage.indexes[name] = i
	defaultStorage.idx.Unlock()
	return i
}

func initDefault() {
	if !defaultInit {
		defaultStorage = New()
	}
}

// GetIndex operates on the defaultDatastore and returns the given Index or nil.
func GetIndex(name string) *Index {
	if !defaultInit {
		return nil
	}
	return defaultStorage.Get(name)
}

// GetDB returns the specifed Database or error or not found.
func (i *Index) GetDB(db string) *Database {
	database, ok := i.db[db]
	if !ok {
		return &Database{
			err: Errf(ErrUnknownDB, "%v", db),
		}
	}
	return database
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
		entries: make(map[string]Entry),
	}
}

// GetEntry returns the specified Entry from the Database.
func (db *Database) GetEntry(entry string) Entry {
	if db.err != nil {
		return &Data{
			err: db.err,
		}
	}
	data, ok := db.entries[entry]
	if !ok {
		return &Data{
			err: Errf(ErrUnknownEntry, "%v", entry),
		}
	}
	return data
}

// AddData adds a Data Entry to the Database.
func (db *Database) AddData(entry string, data interface{}) {
	db.entries[entry] = &Data{Item: data}
}

// AddEntry adds an Entry to the Database.
func (db *Database) AddEntry(entry string, data Entry) {
	db.entries[entry] = data
}

// DeleteEntry deletes the specified Entry from the Database.
func (db *Database) DeleteEntry(entry string) {
	delete(db.entries, entry)
}

// EntryMap returns the direct EntryMap for the Database.
func (db *Database) EntryMap() *map[string]Entry {
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
