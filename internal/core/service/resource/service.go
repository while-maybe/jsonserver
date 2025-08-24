package resource

import (
	"context"
	"errors"
	"fmt"
	"jsonserver/internal/core/domain"

	"github.com/gofrs/uuid/v5"
)

var (
	ErrDuplicateID       = errors.New("record with this ID already exists")
	ErrEmptyRecordID     = errors.New("record ID canno be empty")
	ErrNoDataProvided    = errors.New("no data provided")
	ErrEmptyRecordKey    = errors.New("record key cannot be empty")
	ErrEmptyResourceName = errors.New("resource name cannot be empty")
	ErrGettingAllRecords = errors.New("cannot get all records")
	ErrWrongResourceType = errors.New("operation not valid for this resource type")
	ErrResourceNotFound  = errors.New("top-level resource not found")
	ErrRecordNotFound    = errors.New("record not found")
)

type resourceService struct {
	resourceRepo Repository
}

func NewService(repo Repository) Service {
	return &resourceService{
		resourceRepo: repo,
	}
}

var _ Service = (*resourceService)(nil)

func (s *resourceService) CheckResourceType(ctx context.Context, resourceName string) ResourceType {
	// The service delegates the call to the repository, which has the actual logic.
	return s.resourceRepo.GetResourceType(ctx, resourceName)
}

func (s *resourceService) GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error) {
	if resourceName == "" {
		return nil, fmt.Errorf("GetAllRecords: %w", ErrEmptyResourceName)
	}

	result, err := s.resourceRepo.GetAllRecords(ctx, resourceName)
	if err != nil {
		return nil, fmt.Errorf("GetAllRecords: %w", ErrGettingAllRecords)
	}

	return result, nil
}

func (s *resourceService) GetRecordByID(ctx context.Context, resourceName, resourceID string) (domain.Record, error) {
	if resourceName == "" {
		return nil, fmt.Errorf("GetRecordByID: %w", ErrEmptyResourceName)
	}

	if resourceID == "" {
		return nil, ErrEmptyRecordID
	}

	return s.resourceRepo.GetRecordByID(ctx, resourceName, resourceID)
}

func (s *resourceService) CreateRecordInCollection(ctx context.Context, resourceName string, data map[string]any) (domain.Record, error) {
	// validate
	if len(data) == 0 {
		return nil, fmt.Errorf("CreateRecordInCollection: %w", ErrNoDataProvided)
	}

	// create a new record
	newRecord, err := domain.NewFromMap(data)
	if err != nil {
		return nil, fmt.Errorf("CreateRecordInCollection: invalid data format: %w", err)
	}

	// check if the newly created record includes an ID and if it already exists
	if recordID, hasID := newRecord.ID(); hasID {

		_, err := s.resourceRepo.GetRecordByID(ctx, resourceName, recordID)

		if err == nil {
			// A nil error means a record was found. This is a duplicate.
			return nil, fmt.Errorf("%w: %s", ErrDuplicateID, recordID)
		}

		// If an error occurred, we must ensure it was ErrNotFound. Any other error is a system failure.
		if !errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("failed to check for duplicate ID: %w", err)
		}

		// If we get here, the error was ErrNotFound, which is good. The ID is available.

	} else {
		// new auto-generated ID if one does not yet exist
		newUUID, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("CreateRecordInCollection: could not generate uuid %w", err)
		}
		newRecord["id"] = newUUID.String()
	}

	// if still here then create the record
	result, err := s.resourceRepo.CreateRecord(ctx, resourceName, newRecord)
	if err != nil {
		return nil, fmt.Errorf("CreateRecordInCollection: could not save to repository: %w", err)
	}

	return result, nil
}

func (s *resourceService) CreateOrUpdateRecordInObject(ctx context.Context, resourceName, recordKey string, data map[string]any) (domain.Record, error) {
	// validate
	if len(data) == 0 {
		return nil, fmt.Errorf("CreateOrUpdateRecordInObject: %w", ErrNoDataProvided)
	}
	if recordKey == "" {
		return nil, fmt.Errorf("CreateOrUpdateRecordInObject: %w", ErrEmptyRecordKey)
	}

	// create a new object record
	newRecord, err := domain.NewFromMap(data)
	if err != nil {
		return nil, fmt.Errorf("CreateRecordInCollection: invalid data format: %w", err)
	}

	// if still here then create the record
	result, err := s.resourceRepo.UpsertRecordByKey(ctx, resourceName, recordKey, newRecord)
	if err != nil {
		return nil, fmt.Errorf("CreateRecordInCollection: could not save to repository: %w", err)
	}

	return result, nil
}
