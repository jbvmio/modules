package storage

// Response contains the response from a Request
type Response struct {
	Failure   bool
	HasObject bool
	Object
}
