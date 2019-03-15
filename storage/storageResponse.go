package storage

type Failure bool
type HasObject bool

// StorageResponse contains the response from a StorageRequest
type StorageResponse struct {
	Failure
	HasObject
	Object
}
