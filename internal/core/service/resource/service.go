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
	ErrNoDataProvided    = errors.New("no data provided")
	ErrEmptyRecordKey    = errors.New("record key cannot be empty")
	ErrEmptyResourceName = errors.New("resource name cannot be empty")
	ErrGettingAllRecords = errors.New("cannot get all records")
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

	result, err := s.resourceRepo.GetRecordByID(ctx, resourceName, resourceID)
	if err != nil {
		return nil, fmt.Errorf("GetRecordByID: %w", ErrNotFound)
	}

	return result, nil
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

		existing, err := s.resourceRepo.GetRecordByID(ctx, resourceName, recordID)
		// check if the user provided ID already exists
		if err == nil && existing != nil {
			// given ID already exists
			return nil, fmt.Errorf("%w: %s", ErrDuplicateID, recordID)
		}

		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("CreateRecordInCollection: failed to check for duplicate ID: %w", err)
		}

		// if we're still the given ID is good and we can create the record

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
