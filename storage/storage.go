package storage

import (
	"encoding/json"
	"sync"
	"time"
)

// isLoaded - true if a storage Module is loaded.
var isLoaded bool

// IsLoaded returns true if a storage Module is loaded.
func IsLoaded() bool {
	return isLoaded
}

// RequestConstant is used in Request to indicate the type of request. Numeric ordering is not important
type RequestConstant int

const (
	// StorageSetIndex is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	StorageSetIndex RequestConstant = 0

	// StorageSetData is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	StorageSetData RequestConstant = 1

	// StorageSetDeleteEntry is the request type to remove a topic from the broker and all consumers. Requires Cluster,
	// Group, and Topic fields
	StorageSetDeleteEntry RequestConstant = 2

	// StorageFetchIndexes is the request type to retrieve a list of clusters. Requires Reply. Returns a []string
	StorageFetchIndexes RequestConstant = 3

	// StorageFetchEntries is the request type to retrieve a list of topics in a cluster. Requires Reply and Cluster
	// fields. Returns a []string
	StorageFetchEntries RequestConstant = 4

	// StorageFetchEntry is the request type to retrieve the current broker offsets (one per partition) for a topic.
	// Requires Reply, Cluster, and Topic fields.
	// Returns a []int64
	StorageFetchEntry RequestConstant = 5

	// StorageClearData is the request type to remove all partition owner information for a single group.
	// Requires Cluster and Group fields
	StorageClearData RequestConstant = 6
)

var storageRequestStrings = [...]string{
	"StorageSetIndex",
	"StorageSetData",
	"StorageSetDeleteEntry",
	"StorageFetchIndexes",
	"StorageFetchEntries",
	"StorageFetchEntry",
	"StorageClearData",
}

// RequestHandler handles a storage Request.
type RequestHandler func(*Request)

// HandleRequestMap contains the available Storage Request options
// which can be used to assign RequestHandlers. For convenience.
var HandleRequestMap = map[RequestConstant]RequestHandler{
	StorageSetIndex:       nil,
	StorageSetData:        nil,
	StorageSetDeleteEntry: nil,
	StorageFetchIndexes:   nil,
	StorageFetchEntries:   nil,
	StorageFetchEntry:     nil,
	StorageClearData:      nil,
}

// String returns a string representation of a RequestConstant for logging
func (c RequestConstant) String() string {
	if (c >= 0) && (c < RequestConstant(len(storageRequestStrings))) {
		return storageRequestStrings[c]
	}
	return "UNKNOWN"
}

// MarshalText implements the encoding.TextMarshaler interface. The status is the string representation of
// RequestConstant
func (c RequestConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// MarshalJSON implements the json.Marshaler interface. The status is the string representation of
// RequestConstant
func (c RequestConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// Request is sent over the StorageChannel that is stored in the application context. It is a query to either
// send information to the storage subsystem, or retrieve information from it . The RequestType indiciates the
// particular type of request. "Set" and "Clear" requests do not get a response. "Fetch" requests will send a response
// over the Reply channel supplied in the request
type Request struct {
	// The type of request that this struct encapsulates
	RequestType RequestConstant

	// If the RequestType is a "Fetch" request, Reply must contain a channel to receive the response on
	Reply chan interface{}

	// The name of the cluster to which the request applies. Required for all request types except StorageFetchClusters
	Index string

	// The name of the cluster to which the request applies. Required for all request types except StorageFetchClusters
	DB string

	// The name of the cluster to which the request applies. Required for all request types except StorageFetchClusters
	Entry string

	// The timestamp of the request
	Timestamp int64

	// Interface holding data
	Object
}

// Object is the interface which references the data you want to store.
type Object interface {
	// ClearData wipes any desired data within the object when deleting the entire object isn't desired
	ClearData()

	// ID returns a unique identifying string for the object.
	ID() string
}

// Index contains a map of Databases.
type Index struct {
	//idx map[string][]*ring.Ring // Future Feature*
	db map[string]*Database

	// This lock is used when modifying broker topics or offsets
	idxLock *sync.RWMutex

	// This lock is used when modifying the overall consumer list
	// It does not need to be held for modifying an individual group
	dbLock *sync.RWMutex
}

// NewIndex returns a new Index.
func NewIndex() *Index {
	return &Index{
		//idx:     make(map[string][]*ring.Ring),
		db:      make(map[string]*Database),
		idxLock: &sync.RWMutex{},
		//dbLock:  &sync.RWMutex{},
	}
}

// GetDB returns the specifed Database or error or not found.
func (i *Index) GetDB(db string) (*Database, error) {
	database, ok := i.db[db]
	if !ok {
		return nil, Errf(ErrUnknownDB, "%v", db)
		//return nil, fmt.Errorf("unknown index or db: %v", db)
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

// Database contains a map of Objects.
type Database struct {
	// This lock is held when using the individual group, either for read or write
	lock       *sync.RWMutex
	entries    map[string]*Data
	lastAccess int64
}

// NewDatabase returns a new Database.
func NewDatabase() *Database {
	return &Database{
		lock:    &sync.RWMutex{},
		entries: make(map[string]*Data),
	}
}

// GetEntry returns the specified Entry from the Database.
func (db *Database) GetEntry(entry string) (*Data, error) {
	data, ok := db.entries[entry]
	if !ok {
		return nil, Errf(ErrUnknownEntry, "%v", entry)
		//return nil, fmt.Errorf("unknown entry: %v", entry)
	}
	return data, nil
}

// AddEntry returns the specified Entry from the Database.
func (db *Database) AddEntry(entry string, data *Data) {
	db.entries[entry] = data
}

// EntryMap returns the specified underlying EntryMap for the Database.
func (db *Database) EntryMap() *map[string]*Data {
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

// Data holds the storage Object
type Data struct {
	Object
}

// TimeoutSendStorageRequest is a helper func for sending a protocol.Request to a channel with a timeout,
// specified in seconds. If the request is sent, return true. Otherwise, if the timeout is hit, return false.
func TimeoutSendStorageRequest(storageChannel chan *Request, request *Request, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case storageChannel <- request:
		return true
	case <-timeout:
		return false
	}
}
