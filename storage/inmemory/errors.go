package inmemory

import "fmt"

// ErrCode is a numerical code for an error.
type ErrCode int

// ErrCode Constants
const (
	ErrUnknownIndex     = 0
	ErrUnknownDB        = 1
	ErrUnknownIndexOrDB = 2
	ErrUnknownEntry     = 3
)

// ErrMap contains a map of codes to error string.
var ErrMap = map[ErrCode]string{
	ErrUnknownIndex:     "unknown index",
	ErrUnknownDB:        "unknown db",
	ErrUnknownIndexOrDB: "unknown index or db",
	ErrUnknownEntry:     "unknown entry",
}

// Err implements error interface.
type Err struct {
	err  string
	code ErrCode
}

// Error returns the error string.
func (e Err) Error() string {
	return e.err
}

// Code returns the error code.
func (e Err) Code() ErrCode {
	return e.code
}

// GetErr returns the corresponding Err that corresponds to the given ErrCode.
func GetErr(code ErrCode) Err {
	return Err{
		err:  ErrMap[code],
		code: code,
	}
}

// Errf constructs an Storage Err error.
func Errf(code ErrCode, format string, v ...interface{}) Err {
	var errMsg string
	switch {
	case len(v) > 0:
		errMsg = ErrMap[code] + `: ` + fmt.Sprintf(format, v...)
	default:
		errMsg = ErrMap[code]
	}
	return Err{
		err:  errMsg,
		code: code,
	}
}
