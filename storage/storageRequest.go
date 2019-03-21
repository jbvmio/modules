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

// StorageRequest represents a storage Request type.
type StorageRequest interface {
	Validate() (*Request, bool)
}

// Validate validates a storage Request type and returns true if valid.
func Validate(sr StorageRequest) bool {
	_, ok := sr.Validate()
	return ok
}

// CreateRequest either converts a RequestBuilder to a Request or validates an existing storage Request and returns it back.
func CreateRequest(sr StorageRequest) (*Request, bool) {
	return sr.Validate()
}

// BuildRequest returns a RequestBuilder which can be used to chain construct a Request.
func BuildRequest() *RequestBuilder {
	return new(RequestBuilder)
}

// SetRequestType sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestType(requestType RequestConstant) *RequestBuilder {
	switch requestType {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		sr.Reply = make(chan interface{})
	}
	sr.RequestType = requestType
	return sr
}

/*
// SetRequestSetIndex sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestSetIndex() *RequestBuilder {
	sr.RequestType = StorageSetIndex
	return sr
}

// SetRequestSetEntry sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestSetEntry() *RequestBuilder {
	sr.RequestType = StorageSetEntry
	return sr
}

// SetRequestSetDeleteEntry sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestSetDeleteEntry() *RequestBuilder {
	sr.RequestType = StorageSetDeleteEntry
	return sr
}

// SetRequestFetchIndexes sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestFetchIndexes() *RequestBuilder {
	sr.RequestType = StorageFetchIndexes
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestFetchEntries sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestFetchEntries() *RequestBuilder {
	sr.RequestType = StorageFetchEntries
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestFetchEntry sets the Corresponding Request Type.
func (sr *RequestBuilder) SetRequestFetchEntry() *RequestBuilder {
	sr.RequestType = StorageFetchEntry
	sr.Reply = make(chan interface{})
	return sr
}
*/

// SetIndex sets the index for the Storage Request.
func (sr *RequestBuilder) SetIndex(index string) *RequestBuilder {
	sr.Index = index
	return sr
}

// SetDB sets the DB for the Storage Request.
func (sr *RequestBuilder) SetDB(db string) *RequestBuilder {
	sr.DB = db
	return sr
}

// SetEntry sets the entry for the Storage Request.
func (sr *RequestBuilder) SetEntry(entry string) *RequestBuilder {
	sr.Entry = entry
	return sr
}

// SetObject sets the object for the Storage Request.
func (sr *RequestBuilder) SetObject(obj Object) *RequestBuilder {
	sr.Object = obj
	return sr
}

// Validate validates the RequestBuilder for all fields and returns
// back a converted Request and true if valdation passes.
func (sr *RequestBuilder) Validate() (*Request, bool) {
validateRequest:
	switch sr.RequestType {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		switch {
		case sr.Reply == nil:
			fmt.Println("1")
			break validateRequest
		case sr.DB == "" || sr.Index == "":
			fmt.Println("2")
			if sr.RequestType == TypeFetchEntries || sr.RequestType == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		case sr.Entry == "":
			fmt.Println("3")
			if sr.RequestType == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType == TypeFetchIndexes {
				if sr.DB != "" || sr.Index != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			if sr.RequestType == TypeFetchEntries {
				if sr.Entry != "" {
					break validateRequest
				}
			}
			fmt.Println("4:", sr.RequestType)
			return convertFromBuilder(sr), true
		}
	case TypeSetIndex, TypeSetEntry, TypeDeleteEntry:
		switch {
		case sr.Reply != nil:
			fmt.Println("1")
			break validateRequest
		case sr.Index == "":
			fmt.Println("2")
			break validateRequest
		case sr.DB == "" || sr.Entry == "":
			fmt.Println("3")
			if sr.RequestType == TypeSetEntry || sr.RequestType == TypeDeleteEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType == TypeSetIndex {
				if sr.DB != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			fmt.Println("4:", sr.RequestType)
			return convertFromBuilder(sr), true
		}
	}
	return convertFromBuilder(sr), false
}

func convertFromBuilder(sr *RequestBuilder) *Request {
	return &Request{
		RequestType: sr.RequestType,
		Reply:       sr.Reply,
		Index:       sr.Index,
		DB:          sr.DB,
		Entry:       sr.Entry,
		Timestamp:   sr.Timestamp,
		Object:      sr.Object,
	}
}

// Validate validates the Storage Request for all fields and returns
// it back and true if valdation passes.
func (sr *Request) Validate() (*Request, bool) {
validateRequest:
	switch sr.RequestType {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		switch {
		case sr.Reply == nil:
			fmt.Println("1")
			break validateRequest
		case sr.DB == "" || sr.Index == "":
			fmt.Println("2")
			if sr.RequestType == TypeFetchEntries || sr.RequestType == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		case sr.Entry == "":
			fmt.Println("3")
			if sr.RequestType == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType == TypeFetchIndexes {
				if sr.DB != "" || sr.Index != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			if sr.RequestType == TypeFetchEntries {
				if sr.Entry != "" {
					break validateRequest
				}
			}
			fmt.Println("4:", sr.RequestType)
			return sr, true
		}
	case TypeSetIndex, TypeSetEntry, TypeDeleteEntry:
		switch {
		case sr.Reply != nil:
			fmt.Println("1")
			break validateRequest
		case sr.Index == "":
			fmt.Println("2")
			break validateRequest
		case sr.DB == "" || sr.Entry == "":
			fmt.Println("3")
			if sr.RequestType == TypeSetEntry || sr.RequestType == TypeDeleteEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType == TypeSetIndex {
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
