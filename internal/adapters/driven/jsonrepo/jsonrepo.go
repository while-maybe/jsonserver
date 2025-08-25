package jsonrepo

import (
	"context"
	"log"

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

func (r *jsonRepository) normaliseLoadedValue(value any) any {
	switch v := value.(type) {
	case []any:
		// this is a collection
		normalisedSlice := make([]any, len(v))

		for i, item := range v {
			if itemMap, ok := item.(map[string]any); ok {
				normalisedSlice[i] = domain.Record(itemMap)

			} else {
				// non-map items are kept as is
				normalisedSlice[i] = item
			}
		}
		return normalisedSlice

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

// persist writes the entire in-memory cache back to the JSON file
func (r *jsonRepository) persist() error {

	f, err := os.OpenFile(r.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFilePermissions)
	if err != nil {
		return err
	}
	defer f.Close()

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))

	return jsonv2.MarshalWrite(f, r.data, opts)
}

func (r *jsonRepository) GetResourceType(ctx context.Context, resourceName string) resource.ResourceType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, hasData := r.data[resourceName]
	if !hasData {
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

func (r *jsonRepository) GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, hasData := r.data[resourceName]
	if !hasData {
		// non-existent resource is not an error
		return []domain.Record{}, nil
	}

	// account for the different data structures
	switch value := data.(type) {

	case []any:
		log.Printf("GetAllRecords for %s: Found a collection.", resourceName)

		result := make([]domain.Record, 0, len(value))

		for i, item := range value {
			if record, ok := item.(domain.Record); ok {
				result = append(result, record)
				log.Printf("GetAllRecords: Item %d: %+v", i, record)

			} else {
				log.Printf("WARN: non-record item found in collection: '%s' at index %d, type: %T", resourceName, i, item)
			}
		}
		return result, nil

	case map[string]any:
		log.Printf("GetAllRecords for %s: Found a keyed object. Transforming to a slice.", resourceName)

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
			record := domain.Record(recordMap)

			if _, hasID := record.ID(); !hasID {
				record["id"] = recordID
			}
			return record, nil
		}
		return nil, resource.ErrNotFound

	default:
		// if we got this far, def not found
		return nil, resource.ErrWrongResourceType
	}
}

func (r *jsonRepository) CreateRecord(ctx context.Context, resourceName string, recordData domain.Record) (domain.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, hasData := r.data[resourceName]

	if hasData {
		collection, ok := data.([]any)

		if !ok {
			return nil, resource.ErrWrongResourceType

		}

		if newID, hasID := recordData.ID(); hasID {
			for _, item := range collection {

				if existingRecord, ok := item.(domain.Record); ok {

					if existingID, hasExistingID := existingRecord.ID(); hasExistingID && existingID == newID {

						return nil, resource.ErrDuplicateID
					}
				}
			}
		}

		r.data[resourceName] = append(collection, recordData)

	} else {
		r.data[resourceName] = []any{recordData}
	}

	if err := r.persist(); err != nil {
		return nil, err
	}
	return recordData, nil
}

// TODO implement remaining methods
func (r *jsonRepository) UpsertRecordByKey(ctx context.Context, resourceName, recordKey string, recordData domain.Record) (domain.Record, error) {
	return nil, nil
}
