package jsonrepo

import (
	"context"
	"fmt"
	"log"
	"maps"
	"math"

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

			processed := r.normaliseLoadedValue(item)

			if itemMap, ok := processed.(map[string]any); ok {
				return domain.Record(itemMap)
			}

			// non-map items are kept as is
			return processed
		}
		return r.transformSliceItems(v, op)

	case map[string]any:
		result := make(map[string]any)

		for k, value := range v {
			result[k] = r.normaliseLoadedValue(value)
		}

		return result

	case float64:
		// if the number has no fractional part, convert and return to int
		if v == math.Trunc(v) {
			return int(v)
		}
		// otherwise keep the float
		return v

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

func (r *jsonRepository) findRecordByID(collection []any, recordID string) (domain.Record, int) {

	for i, item := range collection {

		if record, ok := item.(domain.Record); ok {

			if id, ok := record.ID(); ok && id == recordID {
				return record, i
			}
		}
	}

	return nil, -1
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
	if recordID == "" {
		return nil, resource.ErrEmptyRecordKey
	}

	if resourceName == "" {
		return nil, resource.ErrEmptyResourceName
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	data, ok := r.data[resourceName]
	if !ok {
		return nil, resource.ErrResourceNotFound
	}

	// Use direct type switches in methods that need to work with the data  to avoid redundant type assertions and improve readability: The getResourceType helper is reserved for the public API.
	// if we did switch r.getResourceType(data) we'd have to manually assert again
	switch value := data.(type) {
	case []any:

		record, targetIndex := r.findRecordByID(value, recordID)
		if targetIndex == -1 {
			return nil, resource.ErrRecordNotFound
		}

		return record, nil

	case map[string]any:
		item, ok := value[recordID]
		if !ok {
			return nil, resource.ErrRecordNotFound
		}

		if recordMap, ok := item.(domain.Record); ok {

			newRecord := make(domain.Record, len(recordMap)+1)

			// A shallow copy is a better choice otherwise we'd be modifying the recordMap under a Rlock() -  as of 1.21, maps.Copy replaces the need for a loop
			maps.Copy(newRecord, recordMap)

			if _, hasID := newRecord.ID(); !hasID {
				newRecord.SetID(recordID)
			}
			return newRecord, nil
		}

		log.Printf(
			"FATAL: Data corruption detected in resource '%s'. Key '%s' should be a domain.Record but is type %T.", resourceName, recordID, item)
		return nil, resource.ErrInternal

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

	recordToStore := make(domain.Record, len(recordData))
	maps.Copy(recordToStore, recordData)

	r.mu.Lock()
	defer r.mu.Unlock()

	data, hasData := r.data[resourceName]

	if !hasData {
		r.data[resourceName] = []any{recordToStore}

		if err := r.persist(); err != nil {

			// revert the change if saving to file fails
			delete(r.data, resourceName)
			return nil, err
		}
		return recordToStore, nil
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

	newCollection := append(collection, recordToStore)

	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		r.data[resourceName] = collection

		log.Printf("Failed to persist data to %s: %v", r.filename, err)
		return nil, err
	}
	return recordToStore, nil
}

func (r *jsonRepository) UpsertRecordByKey(ctx context.Context, resourceName, recordKey string, recordData domain.Record) (domain.Record, bool, error) {
	wasCreated := false

	if err := recordData.Validate(); err != nil {
		return nil, wasCreated, fmt.Errorf("%w: %s", resource.ErrInvalidRecord, err.Error())
	}

	if recordKey == "" {
		return nil, wasCreated, resource.ErrEmptyRecordKey
	}

	if resourceName == "" {
		return nil, wasCreated, resource.ErrInvalidResourceName
	}

	recordToStore := make(domain.Record, len(recordData))
	maps.Copy(recordToStore, recordData)

	const IDField = "id"
	// Remove ID field since keyed objects use the key as the identifier
	delete(recordToStore, IDField)

	r.mu.Lock()
	defer r.mu.Unlock()

	var originalResource any
	var resourceExists bool
	originalResource, resourceExists = r.data[resourceName]

	keyedObject, isMap := originalResource.(map[string]any)
	// the resource already exists but it's not a map
	if !isMap && resourceExists {
		return nil, wasCreated, resource.ErrWrongResourceType
	}

	// resource did not exist, create it
	if !resourceExists {
		keyedObject = make(map[string]any)
	}

	// we need this so that we return the correct wasCreated bool as it will determine returned http.StatusCode (201 vs 200) in the handler
	_, keyExisted := keyedObject[recordKey]
	wasCreated = !keyExisted

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
		return nil, wasCreated, err
	}

	return recordToStore, wasCreated, nil
}

func (r *jsonRepository) DeleteRecordFromCollection(ctx context.Context, resourceName, recordID string) error {

	if recordID == "" {
		return resource.ErrEmptyRecordID
	}

	if resourceName == "" {
		return resource.ErrEmptyResourceName
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	data, ok := r.data[resourceName]
	if !ok {
		return resource.ErrResourceNotFound
	}

	collection, isCollection := data.([]any)
	if !isCollection {
		return resource.ErrWrongResourceType
	}

	_, targetIndex := r.findRecordByID(collection, recordID)
	if targetIndex == -1 {
		return resource.ErrRecordNotFound
	}

	newCollection := append(collection[:targetIndex], collection[targetIndex+1:]...)
	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {

		r.data[resourceName] = collection

		log.Printf("Failed to persist data to %s: %v", r.filename, err)
		return err
	}

	return nil
}

func (r *jsonRepository) DeleteRecordByKey(ctx context.Context, resourceName, recordKey string) error {

	if recordKey == "" {
		return resource.ErrEmptyRecordKey
	}

	if resourceName == "" {
		return resource.ErrEmptyResourceName
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	originalResource, ok := r.data[resourceName]
	if !ok {
		return resource.ErrResourceNotFound
	}

	keyedObject, isMap := originalResource.(map[string]any)
	if !isMap {
		return resource.ErrWrongResourceType
	}

	if _, keyExists := keyedObject[recordKey]; !keyExists {
		return resource.ErrRecordNotFound
	}

	newKeyedObject := make(map[string]any, len(keyedObject))
	maps.Copy(newKeyedObject, keyedObject)

	delete(newKeyedObject, recordKey)

	r.data[resourceName] = newKeyedObject

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		r.data[resourceName] = originalResource

		log.Printf("Failed to persist deletion of key '%s' from '%s', rolling back: %v", recordKey, resourceName, err)
		return err
	}

	return nil
}

func (r *jsonRepository) UpdateRecordInCollection(ctx context.Context, resourceName, recordID string, recordData domain.Record) (domain.Record, error) {
	if err := recordData.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s", resource.ErrInvalidRecord, err.Error())
	}

	// we can ignore error here has it's been verified with .Validate() above
	if newID, hasID := recordData.ID(); !hasID || newID != recordID {
		return nil, resource.ErrMismatchedID
	}

	recordToStore := make(domain.Record, len(recordData))
	maps.Copy(recordToStore, recordData)

	r.mu.Lock()
	defer r.mu.Unlock()

	data, hasData := r.data[resourceName]

	if !hasData {
		return nil, resource.ErrInvalidResourceName
	}

	collection, ok := data.([]any)
	if !ok {
		return nil, resource.ErrWrongResourceType
	}

	_, recordPos := r.findRecordByID(collection, recordID)
	if recordPos == -1 {
		return nil, resource.ErrRecordNotFound
	}

	newCollection := make([]any, len(collection))
	copy(newCollection, collection)

	newCollection[recordPos] = recordToStore

	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		r.data[resourceName] = collection

		log.Printf("Failed to persist data to %s: %v", r.filename, err)
		return nil, err
	}
	return recordToStore, nil
}
