package storage

// SetRequestSetIndex sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestSetIndex() *StorageRequest {
	sr.RequestType = StorageSetIndex
	return sr
}

// SetRequestSetData sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestSetData() *StorageRequest {
	sr.RequestType = StorageSetData
	return sr
}

// SetRequestSetDeleteEntry sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestSetDeleteEntry() *StorageRequest {
	sr.RequestType = StorageSetDeleteEntry
	return sr
}

// SetRequestFetchIndexes sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestFetchIndexes() *StorageRequest {
	sr.RequestType = StorageFetchIndexes
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestFetchEntries sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestFetchEntries() *StorageRequest {
	sr.RequestType = StorageFetchEntries
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestFetchEntry sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestFetchEntry() *StorageRequest {
	sr.RequestType = StorageFetchEntry
	sr.Reply = make(chan interface{})
	return sr
}

// SetRequestClearData sets the Corresponding Request Type.
func (sr *StorageRequest) SetRequestClearData() *StorageRequest {
	sr.RequestType = StorageClearData
	return sr
}

// SetIndex sets the index for the Storage Request.
func (sr *StorageRequest) SetIndex(index string) *StorageRequest {
	sr.Index = index
	return sr
}

// SetDB sets the DB for the Storage Request.
func (sr *StorageRequest) SetDB(db string) *StorageRequest {
	sr.DB = db
	return sr
}

// SetEntry sets the entry for the Storage Request.
func (sr *StorageRequest) SetEntry(entry string) *StorageRequest {
	sr.Entry = entry
	return sr
}

// SetObject sets the object for the Storage Request.
func (sr *StorageRequest) SetObject(obj Object) *StorageRequest {
	sr.Object = obj
	return sr
}

// Validate validates the Storage Request for all fields and returns
// back the request and false if any fields are missing.
func (sr *StorageRequest) Validate() (*StorageRequest, bool) {
	switch {
	case sr.RequestType < 0 || sr.RequestType > StorageRequestConstant(len(storageRequestStrings)):
		return sr, false
	case sr.Index == "" || sr.DB == "" || sr.Entry == "":
		return sr, false
	}
	return sr, true
}
