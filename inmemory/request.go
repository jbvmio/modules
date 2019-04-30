package inmemory

import "github.com/jbvmio/team"

// RequestConstant is used in Request to indicate the type of request. Numeric ordering is not important
type RequestConstant int

// Object represents various data types
//type Object interface{}

// Response represents any result or response from a Request.
type Response interface{}

// RequestHandleFunc handles Requests.
// The RequestHandleFunc is responsible for handling any Responses required using the Reply channel.
type RequestHandleFunc func(Request)

// RequestMapping is a mapping of RequestConstants and RequestHandlers.
type RequestMapping map[RequestConstant]RequestHandleFunc

const (
	// TypeSetIndex is the request type to Set context to a specific Index.
	TypeSetIndex RequestConstant = 0

	// TypeSetEntry is the request type to Set context to a specific Entry.
	TypeSetEntry RequestConstant = 1

	// TypeDeleteEntry is the request type to remove a specific Entry.
	TypeDeleteEntry RequestConstant = 2

	// TypeFetchIndexes is the request type to retrieve a list of Indexes. Requires Reply. Returns a []string
	TypeFetchIndexes RequestConstant = 3

	// TypeFetchEntries is the request type to retrieve a list of Entries. Requires Reply. Returns a []string
	TypeFetchEntries RequestConstant = 4

	// TypeFetchEntry is the request type to retrieve a specific Entry.
	TypeFetchEntry RequestConstant = 5

	// TypeFetchDatabases is the request type to retrieve a list of Databases. Returns a []string
	TypeFetchDatabases RequestConstant = 6

	// TypeFetchAllEntries is the request type to retrieve all entries in a database. Returns a []interface{}
	TypeFetchAllEntries RequestConstant = 7
)

var storageRequestStrings = [...]string{
	"TypeSetIndex",
	"TypeSetEntry",
	"TypeDeleteEntry",
	"TypeFetchIndexes",
	"TypeFetchEntries",
	"TypeFetchEntry",
	"TypeFetchDatabases",
	"TypeFetchAllEntries",
}

// String returns a string representation of a StorageRequestConstant for logging
func (c RequestConstant) String() string {
	if (c >= 0) && (c < RequestConstant(len(storageRequestStrings))) {
		return storageRequestStrings[c]
	}
	return "UNKNOWN"
}

// RequestID implements team.RequestType.
type RequestID struct {
	id   RequestConstant
	name string
	//consistGroup string
}

func newRequestID(id int, name string) RequestID {
	return RequestID{
		id:   RequestConstant(id),
		name: name,
	}
}

// ID returns the Request ID.
func (r *RequestID) ID() int {
	return int(r.id)
}

// String returns the string representation of the Request ID.
func (r *RequestID) String() string {
	return r.name
}

// Request is sent over the StorageChannel that is stored in the application context. It is a query to either
// send information to the storage subsystem, or retrieve information from it . The RequestType indiciates the
// particular type of request. "Set" and "Clear" requests do not get a response. "Fetch" requests will send a response
// over the Reply channel supplied in the request
type Request struct {
	// The type of request that this struct encapsulates
	RequestType RequestID

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
	Data Entry
}

// ReqType returns the RequestType or ID.
func (r *Request) ReqType() team.RequestType {
	return &r.RequestType
}

// ResultChan returns the Result channel.
func (r *Request) ResultChan() chan interface{} {
	return r.Reply
}

// Get returns the Object.
func (r *Request) Get() interface{} {
	return r.Data
}

// ConsistID is the identifier in the request that is used for consistent requests.
func (r *Request) ConsistID() string {
	return r.DB + r.Entry
}

var requestMap = team.RequestMap{
	All: map[int]team.RequestHandleFunc{
		int(TypeSetIndex):       addIndex,
		int(TypeSetEntry):       addEntry,
		int(TypeDeleteEntry):    deleteEntry,
		int(TypeFetchIndexes):   fetchIndexList,
		int(TypeFetchDatabases): fetchDBList,
		int(TypeFetchEntries):   fetchEntryList,
		int(TypeFetchEntry):     fetchEntry,
	},
	Consistent: map[int]team.RequestHandleFunc{
		int(TypeSetEntry):        addEntry,
		int(TypeDeleteEntry):     deleteEntry,
		int(TypeFetchEntry):      fetchEntry,
		int(TypeFetchAllEntries): fetchAllEntries,
	},
}
