package storage

import (
	"encoding/json"
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
	// TypeSetIndex is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	TypeSetIndex RequestConstant = 0

	// TypeSetEntry is the request type to store a consumer owner. Requires Cluster, Group, Topic, Partition,
	// and Owner fields
	TypeSetEntry RequestConstant = 1

	// TypeDeleteEntry is the request type to remove a topic from the broker and all consumers. Requires Cluster,
	// Group, and Topic fields
	TypeDeleteEntry RequestConstant = 2

	// TypeFetchIndexes is the request type to retrieve a list of clusters. Requires Reply. Returns a []string
	TypeFetchIndexes RequestConstant = 3

	// TypeFetchEntries is the request type to retrieve a list of topics in a cluster. Requires Reply and Cluster
	// fields. Returns a []string
	TypeFetchEntries RequestConstant = 4

	// TypeFetchEntry is the request type to retrieve the current broker offsets (one per partition) for a topic.
	// Requires Reply, Cluster, and Topic fields.
	// Returns a []int64
	TypeFetchEntry RequestConstant = 5
)

var storageRequestStrings = [...]string{
	"TypeSetIndex",
	"TypeSetEntry",
	"TypeDeleteEntry",
	"TypeFetchIndexes",
	"TypeFetchEntries",
	"TypeFetchEntry",
}

// RequestHandler handles a storage Request.
// The RequestHandler is responsible for handling any Responses required by a fetch request using the Reply channel.
type RequestHandler func(*Request)

// NoopHandler can be used to as default RequestHandler.
// It does nothing with the request and closes the Reply channel as needed.
func NoopHandler(request *Request) {
	if request.Reply != nil {
		close(request.Reply)
	}
}

// HandleRequestMap contains the available Storage Request options
// which can be used to assign RequestHandlers. For convenience.
var HandleRequestMap = map[RequestConstant]RequestHandler{
	TypeSetIndex:     nil,
	TypeSetEntry:     nil,
	TypeDeleteEntry:  nil,
	TypeFetchIndexes: nil,
	TypeFetchEntries: nil,
	TypeFetchEntry:   nil,
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
