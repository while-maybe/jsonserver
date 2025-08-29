package resource

import (
	"context"
	"errors"
	"jsonserver/internal/core/domain"
)

var ErrNotFound = errors.New("record not found")

type ResourceType int

const (
	ResourceTypeUnknown ResourceType = iota
	ResourceTypeCollection
	ResourceTypeKeyedObject
	ResourceTypeSingular
)

type Repository interface {
	// Queries
	GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error)
	GetRecordByID(ctx context.Context, resourceName, recordID string) (domain.Record, error)
	// Count(ctx context.Context, resourceName string) (int, error)
	GetResourceType(ctx context.Context, resourceName string) ResourceType

	// Commands for arrays (collections)
	CreateRecord(ctx context.Context, resourceName string, recordData domain.Record) (domain.Record, error)
	// UpdateRecord(ctx context.Context, resourceName, recordID string, recordData domain.Record) (domain.Record, error)
	DeleteRecordFromCollection(ctx context.Context, resourceName, recordID string) error

	// Commands for (keyed) objects
	UpsertRecordByKey(ctx context.Context, resourceName, recordKey string, recordData domain.Record) (domain.Record, bool, error)
	// DeleteRecordByKey(ctx context.Context, resourceName, recordKey string) error

	// Commands for singular resources like ints, str
	// GetSingularResource(ctx context.Context, resourceName string) (any, error)
	// UpdateSingularResource(ctx context.Context, resourceName string, value any) error
}
