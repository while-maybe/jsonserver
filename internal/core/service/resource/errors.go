package resource

import "errors"

var (
	// id/key errors
	ErrDuplicateID    = errors.New("record with this ID already exists")
	ErrEmptyRecordID  = errors.New("record ID cannot be empty")
	ErrMismatchedID   = errors.New("ID in record and parameter provided ID do not match")
	ErrEmptyRecordKey = errors.New("record key cannot be empty")

	// resource errors
	ErrEmptyResourceName   = errors.New("resource name cannot be empty")
	ErrWrongResourceType   = errors.New("operation not valid for this resource type")
	ErrInvalidResourceName = errors.New("resource name is invalid")
	ErrUnknownResourceType = errors.New("unknown resource type")
	ErrResourceNotFound    = errors.New("top-level resource not found")

	// record errors
	ErrGettingAllRecords = errors.New("cannot get all records")
	ErrRecordNotFound    = errors.New("record not found")
	ErrInvalidRecord     = errors.New("record is invalid")
	ErrNoDataProvided    = errors.New("no data provided")

	// others
	ErrInternal = errors.New("internal server error")
)
