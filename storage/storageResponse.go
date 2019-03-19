package storage

type Failure bool
type HasObject bool

// Response contains the response from a Request
type Response struct {
	Failure
	HasObject
	Object
}
