package resource

import (
	"context"
	"errors"
	"fmt"
	"jsonserver/internal/core/domain"

	"github.com/gofrs/uuid/v5"
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
		return nil, ErrEmptyResourceName
	}

	result, err := s.resourceRepo.GetAllRecords(ctx, resourceName)
	if err != nil {
		return nil, fmt.Errorf("GetAllRecords: %w", ErrGettingAllRecords)
	}

	return result, nil
}

func (s *resourceService) GetRecordByID(ctx context.Context, resourceName, recordID string) (domain.Record, error) {
	if resourceName == "" {
		return nil, ErrEmptyResourceName
	}

	if recordID == "" {
		return nil, ErrEmptyRecordID
	}

	return s.resourceRepo.GetRecordByID(ctx, resourceName, recordID)
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

		// If an error occurred, we must ensure it was NOT ErrNotFound. Any other error is a system failure.
		if !errors.Is(err, ErrRecordNotFound) {
			return nil, fmt.Errorf("failed to check for duplicate ID: %w", err)
		}

		// If we get here, the error was ErrNotFound, which is good. The ID is available.

	} else {
		// new auto-generated ID if one does not yet exist
		newUUID, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("CreateRecordInCollection: could not generate uuid %w", err)
		}
		newRecord.SetID(newUUID.String())
	}

	// if still here then create the record
	result, err := s.resourceRepo.CreateRecord(ctx, resourceName, newRecord)
	if err != nil {
		return nil, fmt.Errorf("CreateRecordInCollection: could not save to repository: %w", err)
	}

	return result, nil
}

func (s *resourceService) UpsertRecordInObject(ctx context.Context, resourceName, recordKey string, data map[string]any) (domain.Record, bool, error) {
	wasCreated := false
	// validate
	if len(data) == 0 {
		return nil, wasCreated, fmt.Errorf("UpsertRecordInObject: %w", ErrNoDataProvided)
	}
	if recordKey == "" {
		return nil, wasCreated, fmt.Errorf("UpsertRecordInObject: %w", ErrEmptyRecordKey)
	}

	// create a new object record
	newRecord, err := domain.NewFromMap(data)
	if err != nil {
		return nil, wasCreated, ErrInvalidRecord
	}

	// if still here then create the record
	var result domain.Record
	result, wasCreated, err = s.resourceRepo.UpsertRecordByKey(ctx, resourceName, recordKey, newRecord)
	if err != nil {
		return nil, wasCreated, fmt.Errorf("UpsertRecordInObject: could not save to repository: %w", err)
	}

	return result, wasCreated, nil
}

func (s *resourceService) DeleteRecordFromCollection(ctx context.Context, resourceName string, recordID string) error {
	if resourceName == "" {
		return ErrEmptyResourceName
	}

	if recordID == "" {
		return ErrEmptyRecordID
	}

	return s.resourceRepo.DeleteRecordFromCollection(ctx, resourceName, recordID)
}

func (s *resourceService) DeleteRecordInObject(ctx context.Context, resourceName, recordKey string) error {
	if resourceName == "" {
		return ErrEmptyResourceName
	}

	if recordKey == "" {
		return ErrEmptyRecordKey
	}

	return s.resourceRepo.DeleteRecordByKey(ctx, resourceName, recordKey)
}

func (s *resourceService) UpdateRecordInCollection(ctx context.Context, resourceName string, recordID string, data map[string]any) (domain.Record, error) {
	if resourceName == "" {
		return nil, ErrEmptyResourceName
	}

	if recordID == "" {
		return nil, ErrEmptyRecordID
	}

	if len(data) == 0 {
		return nil, ErrNoDataProvided
	}

	// create a new record for the request data (the data func parameter)
	recordFromRequest, err := domain.NewFromMap(data)
	if err != nil {
		return nil, fmt.Errorf("UpdateRecordInCollection: invalid data format: %w", err)
	}

	if bodyID, hasID := recordFromRequest.ID(); hasID {
		// if the record (in post data) has an ID, it must match the recordID (from the parameter)
		if bodyID != recordID {
			return nil, fmt.Errorf("%w: id in request body ('%s') does not match id in URL ('%s')", ErrInvalidRecord, bodyID, recordID)
		}
	} else {
		// there is no 'id' field in postData, so we force it to be the provided parameter recordID
		recordFromRequest.SetID(recordID)
	}

	updatedRecord, err := s.resourceRepo.UpdateRecordInCollection(ctx, resourceName, recordID, recordFromRequest)
	if err != nil {
		return nil, fmt.Errorf("UpdateRecordInCollection: %w", err)
	}

	return updatedRecord, nil
}

func (s *resourceService) ListResources(ctx context.Context) ([]string, error) {

	return s.resourceRepo.ListResources(ctx)
}
