package resource

import (
	"context"
	"jsonserver/internal/core/domain"
)

type Service interface {
	// Queries
	GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error)
	GetRecordByID(ctx context.Context, resourceName, recordID string) (domain.Record, error)
	// Count(ctx context.Context, resourceName string) (int, error)
	CheckResourceType(ctx context.Context, resourceName string) ResourceType
	ListResources(ctx context.Context) ([]string, error)

	// Commands for arrays (collections)
	CreateRecordInCollection(ctx context.Context, resourceName string, data map[string]any) (domain.Record, error)
	UpdateRecordInCollection(ctx context.Context, resourceName string, recordID string, data map[string]any) (domain.Record, error)
	DeleteRecordFromCollection(ctx context.Context, resourceName string, recordID string) error

	// Commands for (keyed) objects
	UpsertRecordInObject(ctx context.Context, resourceName, recordKey string, data map[string]any) (domain.Record, bool, error)
	DeleteRecordInObject(ctx context.Context, resourceName, recordKey string) error

	// Commands for singular resources like ints, str
	// GetSingularResource(ctx context.Context, resourceName string) (any, error)
	// UpdateSingularResource(ctx context.Context, resourceName string, value any) (any, error)
}
