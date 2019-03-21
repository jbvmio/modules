package storage

import (
	"fmt"
	"time"
)

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

// RequestBuilder helps build a Request using chains.
// Using Validate will return a Request.
type RequestBuilder struct {
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
	// ID returns a unique identifying string for the object.
	ID() string
}

// SetRequestSetIndex sets the Corresponding Request Type.
func (sr *Request) SetRequestSetIndex() *Request {
	sr.RequestType = StorageSetIndex
	return sr
}

// SetRequestSetEntry sets the Corresponding Request Type.
func (sr *Request) SetRequestSetEntry() *Request {
	sr.RequestType = StorageSetEntry
	return sr
}

// SetRequestSetDeleteEntry sets the Corresponding Request Type.
func (sr *Request) SetRequestSetDeleteEntry() *Request {
	sr.RequestType = StorageSetDeleteEntry
	return sr
}

// SetRequestFetchIndexes sets the Corresponding Request Type.
func (sr *Request) SetRequestFetchIndexes() *Request {
	sr.RequestType = StorageFetchIndexes
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestFetchEntries sets the Corresponding Request Type.
func (sr *Request) SetRequestFetchEntries() *Request {
	sr.RequestType = StorageFetchEntries
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestFetchEntry sets the Corresponding Request Type.
func (sr *Request) SetRequestFetchEntry() *Request {
	sr.RequestType = StorageFetchEntry
	sr.Reply = make(chan interface{})
	return sr
}

// SetIndex sets the index for the Storage Request.
func (sr *Request) SetIndex(index string) *Request {
	sr.Index = index
	return sr
}

// SetDB sets the DB for the Storage Request.
func (sr *Request) SetDB(db string) *Request {
	sr.DB = db
	return sr
}

// SetEntry sets the entry for the Storage Request.
func (sr *Request) SetEntry(entry string) *Request {
	sr.Entry = entry
	return sr
}

// SetObject sets the object for the Storage Request.
func (sr *Request) SetObject(obj Object) *Request {
	sr.Object = obj
	return sr
}

// Validate validates the Storage Request for all fields and returns
// back the request and true if valdation passes.
func (sr *Request) Validate() (*Request, bool) {
validateRequest:
	switch sr.RequestType {
	case StorageFetchIndexes, StorageFetchEntries, StorageFetchEntry:
		switch {
		case sr.Reply == nil:
			fmt.Println("1")
			break validateRequest
		case sr.DB == "" || sr.Index == "":
			fmt.Println("2")
			if sr.RequestType == StorageFetchEntries || sr.RequestType == StorageFetchEntry {
				break validateRequest
			}
			fallthrough
		case sr.Entry == "":
			fmt.Println("3")
			if sr.RequestType == StorageFetchEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType == StorageFetchIndexes {
				if sr.DB != "" || sr.Index != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			if sr.RequestType == StorageFetchEntries {
				if sr.Entry != "" {
					break validateRequest
				}
			}
			fmt.Println("4:", sr.RequestType)
			return sr, true
		}
	case StorageSetIndex, StorageSetEntry, StorageSetDeleteEntry:
		switch {
		case sr.Reply != nil:
			fmt.Println("1")
			break validateRequest
		case sr.Index == "":
			fmt.Println("2")
			break validateRequest
		case sr.DB == "" || sr.Entry == "":
			fmt.Println("3")
			if sr.RequestType == StorageSetEntry || sr.RequestType == StorageSetDeleteEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType == StorageSetIndex {
				if sr.DB != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			fmt.Println("4:", sr.RequestType)
			return sr, true
		}
	}
	return sr, false
}

// TimeoutSendStorageRequest sends a Request to a channel with a timeout,
// specified in seconds. If the request is sent, return true. Otherwise, if the timeout is hit, return false.
// A Listener should be available to service the request.
func TimeoutSendStorageRequest(storageChannel chan *Request, request *Request, maxTime int) bool {
	timeout := time.After(time.Duration(maxTime) * time.Second)
	select {
	case storageChannel <- request:
		return true
	case <-timeout:
		return false
	}
}
