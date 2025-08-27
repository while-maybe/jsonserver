package jsonrepo

import (
	"context"
	"fmt"
	"log"
	"maps"

	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"os"
	"sync"
)

// below is placed here so import doesn't complain

type jsonRepository struct {
	filename string
	mu       sync.RWMutex
	data     map[string]any // in-memory cache
}

var _ resource.Repository = (*jsonRepository)(nil)

func NewJsonRepository(filename string) (resource.Repository, error) {
	repo := &jsonRepository{
		filename: filename,
		data:     make(map[string]any),
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if err := repo.load(); err != nil {
		if os.IsNotExist(err) {
			return repo, nil
		}
		// if error is anything else apart from file not existing
		return nil, err
	}

	return repo, nil
}

const defaultFilePermissions = os.FileMode(0644)

func (r *jsonRepository) load() error {

	bytes, err := os.ReadFile(r.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// check for empty file
	if len(bytes) == 0 {
		return nil
	}

	var temp map[string]any
	if err := jsonv2.Unmarshal(bytes, &temp); err != nil {
		return err
	}

	for resourceName, resourceValue := range temp {
		r.data[resourceName] = r.normaliseLoadedValue(resourceValue)
	}

	return nil
}

// persist writes the entire in-memory cache back to the JSON file
func (r *jsonRepository) persist() error {

	f, err := os.OpenFile(r.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFilePermissions)
	if err != nil {
		return err
	}
	defer f.Close()

	denormalisedData := make(map[string]any)
	for resourceName, resourceValue := range r.data {
		denormalisedData[resourceName] = r.denormaliseForPersist(resourceValue)
	}

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))

	return jsonv2.MarshalWrite(f, denormalisedData, opts)
}

// transformSliceItems takes a collection and applies a transformer function to each item returning a same length collection - think .map() in JS
func (r *jsonRepository) transformSliceItems(slice []any, transformer func(any) any) []any {
	result := make([]any, len(slice))

	for i, item := range slice {
		result[i] = transformer(item)
	}

	return result
}

func (r *jsonRepository) normaliseLoadedValue(value any) any {
	switch v := value.(type) {
	case []any:

		op := func(item any) any {
			if itemMap, ok := item.(map[string]any); ok {
				return domain.Record(itemMap)
			}

			// non-map items are kept as is
			return item
		}
		return r.transformSliceItems(v, op)

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

func (r *jsonRepository) denormaliseForPersist(value any) any {
	switch v := value.(type) {
	case []any:

		op := func(item any) any {
			if itemMap, ok := item.(domain.Record); ok {
				return map[string]any(itemMap)
			}

			// non-map items are kept as is
			return item
		}
		return r.transformSliceItems(v, op)

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

func (r *jsonRepository) getResourceType(data any) resource.ResourceType {
	if data == nil {
		return resource.ResourceTypeUnknown
	}

	switch data.(type) {
	case []any:
		return resource.ResourceTypeCollection
	case map[string]any:
		return resource.ResourceTypeKeyedObject
	default:
		return resource.ResourceTypeSingular
	}
}

func (r *jsonRepository) GetResourceType(ctx context.Context, resourceName string) resource.ResourceType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getResourceType(r.data[resourceName])
}

func (r *jsonRepository) GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, hasData := r.data[resourceName]
	if !hasData {
		// non-existent resource is not an error
		return []domain.Record{}, nil
	}

	// Use direct type switches in methods that need to work with the data  to avoid redundant type assertions and improve readability: The getResourceType helper is reserved for the public API.
	// if we did switch r.getResourceType(data) we'd have to manually assert again
	switch value := data.(type) {

	case []any:
		result := make([]domain.Record, 0, len(value))

		for i, item := range value {
			if record, ok := item.(domain.Record); ok {
				result = append(result, record)

			} else {
				log.Printf("WARN: non-record item found in collection: '%s' at index %v, type: %T", resourceName, i, item)
			}
		}
		return result, nil

	case map[string]any:
		result := make([]domain.Record, 0, len(value))

		for key, item := range value {
			record := domain.Record{
				"key":   key,
				"value": item,
			}
			result = append(result, record)
		}
		return result, nil

	default:
		return []domain.Record{}, nil
	}

}

func (r *jsonRepository) GetRecordByID(ctx context.Context, resourceName, recordID string) (domain.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, ok := r.data[resourceName]
	if !ok {
		return nil, resource.ErrNotFound
	}

	// Use direct type switches in methods that need to work with the data  to avoid redundant type assertions and improve readability: The getResourceType helper is reserved for the public API.
	// if we did switch r.getResourceType(data) we'd have to manually assert again
	switch value := data.(type) {
	case []any:
		for _, item := range value {

			if record, ok := item.(domain.Record); ok {

				if id, ok := record.ID(); ok && id == recordID {
					return record, nil
				}
			}
		}
		return nil, resource.ErrNotFound

	case map[string]any:
		item, ok := value[recordID]
		if !ok {
			return nil, resource.ErrNotFound
		}

		if recordMap, ok := item.(map[string]any); ok {

			newRecord := make(domain.Record, len(recordMap)+1)

			// A shallow copy is a better choice otherwise we'd be modifying the recordMap under a Rlock() -  as of 1.21, maps.Copy replaces the need for a loop
			maps.Copy(newRecord, recordMap)

			if _, hasID := newRecord.ID(); !hasID {
				newRecord.SetID(recordID)
			}
			return newRecord, nil
		}
		return nil, resource.ErrNotFound

	default:
		// if we got this far, def not found
		return nil, resource.ErrWrongResourceType
	}
}

func (r *jsonRepository) CreateRecord(ctx context.Context, resourceName string, recordData domain.Record) (domain.Record, error) {
	if err := recordData.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s", resource.ErrInvalidRecord, err.Error())
	}

	newID, hasID := recordData.ID()
	if !hasID {
		return nil, fmt.Errorf("%w: records in a collection must have a valid ID", resource.ErrInvalidRecord)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	data, hasData := r.data[resourceName]

	if !hasData {
		r.data[resourceName] = []any{recordData}

		if err := r.persist(); err != nil {

			// revert the change if saving to file fails
			delete(r.data, resourceName)
			return nil, err
		}
		return recordData, nil
	}

	collection, ok := data.([]any)
	if !ok {
		return nil, resource.ErrWrongResourceType
	}

	for _, item := range collection {

		if existingRecord, ok := item.(domain.Record); ok {

			if existingID, hasExistingID := existingRecord.ID(); hasExistingID && existingID == newID {
				return nil, resource.ErrDuplicateID
			}
		}
	}

	newCollection := append(collection, recordData)
	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		r.data[resourceName] = collection

		log.Printf("Failed to persist data to %s: %v", r.filename, err)
		return nil, err
	}
	return recordData, nil
}

func (r *jsonRepository) UpsertRecordByKey(ctx context.Context, resourceName, recordKey string, recordData domain.Record) (domain.Record, error) {
	if err := recordData.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s", resource.ErrInvalidRecord, err.Error())
	}

	if recordKey == "" {
		return nil, resource.ErrEmptyRecordKey
	}

	recordToStore := make(domain.Record, len(recordData))
	maps.Copy(recordToStore, recordData)

	r.mu.Lock()
	defer r.mu.Unlock()

	var originalResource any
	var resourceExists bool
	originalResource, resourceExists = r.data[resourceName]

	keyedObject, isMap := originalResource.(map[string]any)
	// the resource already exists but it's not a map
	if !isMap && resourceExists {
		return nil, resource.ErrWrongResourceType
	}

	// resource did not exist, create it
	if !resourceExists {
		keyedObject = make(map[string]any)
	}

	keyedObject[recordKey] = recordToStore
	r.data[resourceName] = keyedObject

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		if !resourceExists {
			delete(r.data, resourceName)
		} else {
			r.data[resourceName] = originalResource
		}

		log.Printf("Failed to persist data to %s: %v", r.filename, err)
		return nil, err
	}

	return recordToStore, nil
}

// TODO implement remaining methods
