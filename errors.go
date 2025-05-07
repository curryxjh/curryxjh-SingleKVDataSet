package SingleKVDataSet

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrKeyNotFound            = errors.New("key not found in databse")
	ErrDataFileNotFound       = errors.New("data file not found in databse")
	ErrDataDirectoryCorrupted = errors.New("database data directory corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch num")
	ErrMergeIsProgress        = errors.New("merge is progress, try again later")
	ErrDatabaseIsUsing        = errors.New("the database directory is used by  another process")
)
