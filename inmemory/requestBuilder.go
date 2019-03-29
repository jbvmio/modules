package inmemory

import (
	"time"
)

// RequestBuilder helps build a Request using chains.
// Using Validate will return a Request.
type RequestBuilder struct {
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

// StorageRequest represents a storage Request type.
type StorageRequest interface {
	IsValid() bool
}

// IsValid validates a storage Request type and returns true if valid.
func IsValid(sr StorageRequest) bool {
	return sr.IsValid()
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
	req := RequestID{id: requestType, name: requestType.String()}
	sr.RequestType = req
	return sr
}

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

// AddData attaches any desired Data for the Storage Request.
func (sr *RequestBuilder) AddData(data interface{}) *RequestBuilder {
	sr.Data = &Data{Item: data}
	return sr
}

// AddEntry attaches an Entry type for the Storage Request.
func (sr *RequestBuilder) AddEntry(data Entry) *RequestBuilder {
	sr.Data = data
	return sr
}

// CreateRequest creates a new Request from a RequestBuilder.
// If the RequestType is a Fetch type, the Reply channel is added if not already present.
// This does not validate the Request.
func (sr *RequestBuilder) CreateRequest() *Request {
	switch sr.RequestType.id {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		if sr.Reply == nil {
			sr.Reply = make(chan interface{})
		}
	}
	return convertFromBuilder(sr)
}

// IsValid validates the RequestBuilder for all fields and returns true if valdation passes.
func (sr *RequestBuilder) IsValid() bool {
validateRequest:
	switch sr.RequestType.id {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		switch {
		case sr.Reply == nil:
			break validateRequest
		case sr.DB == "" || sr.Index == "":
			if sr.RequestType.id == TypeFetchEntries || sr.RequestType.id == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		case sr.Entry == "":
			if sr.RequestType.id == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType.id == TypeFetchIndexes {
				if sr.DB != "" || sr.Index != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			if sr.RequestType.id == TypeFetchEntries {
				if sr.Entry != "" {
					break validateRequest
				}
			}
			return true
		}
	case TypeSetIndex, TypeSetEntry, TypeDeleteEntry:
		switch {
		case sr.Reply != nil:
			break validateRequest
		case sr.Index == "":
			break validateRequest
		case sr.DB == "" || sr.Entry == "":
			if sr.RequestType.id == TypeSetEntry || sr.RequestType.id == TypeDeleteEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType.id == TypeSetIndex {
				if sr.DB != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			return true
		}
	}
	return false
}

// CreateRequest takes a RequestBuilder, validates it and returns a Request and true if validation passes.
// If the RequestType is a Fetch type, the Reply channel is added if not already present.
// If validation does not pass, the returned Request will be nil.
func CreateRequest(sr *RequestBuilder) (*Request, bool) {
	switch sr.RequestType.id {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		if sr.Reply == nil {
			sr.Reply = make(chan interface{})
		}
	}
	if ok := sr.IsValid(); ok {
		return convertFromBuilder(sr), ok
	}
	close(sr.Reply)
	return nil, false
}

func convertFromBuilder(sr *RequestBuilder) *Request {
	return &Request{
		RequestType: sr.RequestType,
		Reply:       sr.Reply,
		Index:       sr.Index,
		DB:          sr.DB,
		Entry:       sr.Entry,
		Timestamp:   sr.Timestamp,
		Data:        sr.Data,
	}
}

// IsValid validates the Storage Request for all fields and returns true if valdation passes.
func (sr *Request) IsValid() bool {
validateRequest:
	switch sr.RequestType.id {
	case TypeFetchIndexes, TypeFetchEntries, TypeFetchEntry:
		switch {
		case sr.Reply == nil:
			break validateRequest
		case sr.DB == "" || sr.Index == "":
			if sr.RequestType.id == TypeFetchEntries || sr.RequestType.id == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		case sr.Entry == "":
			if sr.RequestType.id == TypeFetchEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType.id == TypeFetchIndexes {
				if sr.DB != "" || sr.Index != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			if sr.RequestType.id == TypeFetchEntries {
				if sr.Entry != "" {
					break validateRequest
				}
			}
			return true
		}
	case TypeSetIndex, TypeSetEntry, TypeDeleteEntry:
		switch {
		case sr.Reply != nil:
			break validateRequest
		case sr.Index == "":
			break validateRequest
		case sr.DB == "" || sr.Entry == "":
			if sr.RequestType.id == TypeSetEntry || sr.RequestType.id == TypeDeleteEntry {
				break validateRequest
			}
			fallthrough
		default:
			if sr.RequestType.id == TypeSetIndex {
				if sr.DB != "" || sr.Entry != "" {
					break validateRequest
				}
			}
			return true
		}
	}
	return false
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
