package storage

import (
	"encoding/json"
	"time"
)

// isLoaded - true if a storage Module is loaded.
var isLoaded bool

// IsLoaded returns true if a storage Module is loaded.
func IsLoaded() bool {
	return isLoaded
}

// StorageRequestConstant is used in StorageRequest to indicate the type of request. Numeric ordering is not important
type StorageRequestConstant int

const (
	// StorageSetIndex is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	StorageSetIndex StorageRequestConstant = 0

	// StorageSetData is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	StorageSetData StorageRequestConstant = 1

	// StorageSetDeleteEntry is the request type to remove a topic from the broker and all consumers. Requires Cluster,
	// Group, and Topic fields
	StorageSetDeleteEntry StorageRequestConstant = 2

	// StorageFetchIndexes is the request type to retrieve a list of clusters. Requires Reply. Returns a []string
	StorageFetchIndexes StorageRequestConstant = 3

	// StorageFetchEntries is the request type to retrieve a list of topics in a cluster. Requires Reply and Cluster
	// fields. Returns a []string
	StorageFetchEntries StorageRequestConstant = 4

	// StorageFetchEntry is the request type to retrieve the current broker offsets (one per partition) for a topic.
	// Requires Reply, Cluster, and Topic fields.
	// Returns a []int64
	StorageFetchEntry StorageRequestConstant = 5

	// StorageClearData is the request type to remove all partition owner information for a single group.
	// Requires Cluster and Group fields
	StorageClearData StorageRequestConstant = 6
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

// String returns a string representation of a StorageRequestConstant for logging
func (c StorageRequestConstant) String() string {
	if (c >= 0) && (c < StorageRequestConstant(len(storageRequestStrings))) {
		return storageRequestStrings[c]
	}
	return "UNKNOWN"
}

// MarshalText implements the encoding.TextMarshaler interface. The status is the string representation of
// StorageRequestConstant
func (c StorageRequestConstant) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// MarshalJSON implements the json.Marshaler interface. The status is the string representation of
// StorageRequestConstant
func (c StorageRequestConstant) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

// StorageRequest is sent over the StorageChannel that is stored in the application context. It is a query to either
// send information to the storage subsystem, or retrieve information from it . The RequestType indiciates the
// particular type of request. "Set" and "Clear" requests do not get a response. "Fetch" requests will send a response
// over the Reply channel supplied in the request
type StorageRequest struct {
	// The type of request that this struct encapsulates
	RequestType StorageRequestConstant

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
	// ClearData wipes any pertinent data within the object when deleting the entire object isn't desired
	ClearData()

	// ID returns a unique identifying string for the object.
	ID() string
}

// TimeoutSendStorageRequest is a helper func for sending a protocol.StorageRequest to a channel with a timeout,
// specified in seconds. If the request is sent, return true. Otherwise, if the timeout is hit, return false.
func TimeoutSendStorageRequest(storageChannel chan *StorageRequest, request *StorageRequest, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case storageChannel <- request:
		return true
	case <-timeout:
		return false
	}
}