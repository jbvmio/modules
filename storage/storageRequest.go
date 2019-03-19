package storage

// SetRequestSetIndex sets the Corresponding Request Type.
func (sr *Request) SetRequestSetIndex() *Request {
	sr.RequestType = StorageSetIndex
	return sr
}

// SetRequestSetData sets the Corresponding Request Type.
func (sr *Request) SetRequestSetData() *Request {
	sr.RequestType = StorageSetData
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

// SetRequestClearData sets the Corresponding Request Type.
func (sr *Request) SetRequestClearData() *Request {
	sr.RequestType = StorageClearData
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
// back the request and false if any fields are missing.
func (sr *Request) Validate() (*Request, bool) {
	switch {
	case sr.RequestType < 0 || sr.RequestType > RequestConstant(len(storageRequestStrings)):
		return sr, false
	case sr.Index == "" || sr.DB == "" || sr.Entry == "":
		return sr, false
	}
	return sr, true
}
