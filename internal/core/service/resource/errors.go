package resource

import "errors"

var (
	ErrDuplicateID         = errors.New("record with this ID already exists")
	ErrEmptyRecordID       = errors.New("record ID cannot be empty")
	ErrMismatchedID        = errors.New("ID in record and parameter provided ID do not match")
	ErrNoDataProvided      = errors.New("no data provided")
	ErrEmptyRecordKey      = errors.New("record key cannot be empty")
	ErrEmptyResourceName   = errors.New("resource name cannot be empty")
	ErrWrongResourceType   = errors.New("operation not valid for this resource type")
	ErrUnknownResourceType = errors.New("unknown resource type")
	ErrResourceNotFound    = errors.New("top-level resource not found")
	ErrRecordNotFound      = errors.New("record not found")
	ErrInvalidRecord       = errors.New("record is invalid")
	ErrInvalidResourceName = errors.New("resource name is invalid")
	ErrInternal            = errors.New("internal server error")
	ErrGettingAllRecords   = errors.New("cannot get all records")
)
