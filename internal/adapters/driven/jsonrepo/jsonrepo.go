package jsonrepo

import (
	"context"
	"fmt"
	"log"
	"maps"
	"math"
	"sort"

	jsonv2 "encoding/json/v2"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"os"
	"sync"
)

type JsonRepository struct {
	p    Persister // Exported for testing
	mu   sync.RWMutex
	data map[string]any // in-memory cache
}

var _ resource.Repository = (*JsonRepository)(nil)

func NewJsonRepository(filename string) (resource.Repository, error) {

	return NewJsonRepositoryWithPersister(filename, NewFilePersister(filename))
}

// NewJsonRepositoryWithPersister is the constructor for testing or custom persistence. It is EXPORTED so the _test package can call it.
func NewJsonRepositoryWithPersister(filename string, p Persister) (resource.Repository, error) {
	repo := &JsonRepository{
		p:    p,
		data: make(map[string]any),
	}

	if err := repo.loadFromFile(filename); err != nil {
		if os.IsNotExist(err) {
			return repo, nil
		}
		// if error is anything else apart from file not existing
		return nil, err
	}

	return repo, nil
}

func (r *JsonRepository) loadFromFile(filename string) error {

	bytes, err := os.ReadFile(filename)
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
func (r *JsonRepository) persist() error {
	denormalisedData := make(map[string]any)
	for resourceName, resourceValue := range r.data {
		denormalisedData[resourceName] = r.denormaliseForPersist(resourceValue)
	}

	return r.p.Persist(denormalisedData)
}

// transformSliceItems takes a collection and applies a transformer function to each item returning a same length collection - think .map() in JS
func (r *JsonRepository) transformSliceItems(slice []any, transformer func(any) any) []any {
	result := make([]any, len(slice))

	for i, item := range slice {
		result[i] = transformer(item)
	}

	return result
}

func (j *JsonRepository) transformMapItems(m map[string]any, transformer func(any) any) domain.Record {
	normalisedMap := make(map[string]any, len(m))

	for key, value := range m {
		normalisedMap[key] = transformer(value)
	}

	// this conversion is safe as domain.Record IS a map[string]any
	return domain.Record(normalisedMap)
}

func (r *JsonRepository) normaliseLoadedValue(value any) any {
	switch v := value.(type) {
	case float64:
		// if the number has no fractional part, convert and return to int
		if v == math.Trunc(v) {
			return int(v)
		}
		// otherwise keep the float
		return v

	case []any:
		return r.transformSliceItems(v, r.normaliseLoadedValue)

	case map[string]any:
		return r.transformMapItems(v, r.normaliseLoadedValue)

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

func (r *JsonRepository) denormaliseForPersist(value any) any {
	switch v := value.(type) {
	case domain.Record:
		return r.transformMapItems(v, r.denormaliseForPersist)

	case []any:
		return r.transformSliceItems(v, r.denormaliseForPersist)

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

func (r *JsonRepository) getResourceType(data any) resource.ResourceType {
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

func (r *JsonRepository) findRecordByID(collection []any, recordID string) (domain.Record, int) {

	for i, item := range collection {

		if record, ok := item.(domain.Record); ok {

			if id, ok := record.ID(); ok && id == recordID {
				return record, i
			}
		}
	}

	return nil, -1
}

func (r *JsonRepository) asCollection(data any) ([]any, error) {
	collection, ok := data.([]any)
	if !ok {
		return nil, resource.ErrWrongResourceType
	}
	return collection, nil
}

func (r *JsonRepository) asKeyedObject(data any) (domain.Record, error) {
	keyedObject, ok := data.(domain.Record)
	if !ok {
		return nil, resource.ErrWrongResourceType
	}
	return keyedObject, nil
}

func (r *JsonRepository) validateResourceName(name string) error {
	if name == "" {
		return resource.ErrEmptyResourceName
	}
	return nil
}

func (r *JsonRepository) validateRecordID(name string) error {
	if name == "" {
		return resource.ErrEmptyRecordID
	}
	return nil
}

func (r *JsonRepository) validateRecordKey(name string) error {
	if name == "" {
		return resource.ErrEmptyRecordKey
	}
	return nil
}

func (r *JsonRepository) GetResourceType(ctx context.Context, resourceName string) resource.ResourceType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.getResourceType(r.data[resourceName])
}

func (r *JsonRepository) GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error) {
	if err := r.validateResourceName(resourceName); err != nil {
		return nil, err
	}

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

	case domain.Record:
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
		record := domain.Record{
			"key":   resourceName,
			"value": value,
		}
		return []domain.Record{record}, nil
	}
}

func (r *JsonRepository) GetRecordByID(ctx context.Context, resourceName, recordID string) (domain.Record, error) {
	if err := r.validateResourceName(resourceName); err != nil {
		return nil, err
	}

	if err := r.validateRecordID(recordID); err != nil {
		return nil, err
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

	case domain.Record:
		item, ok := value[recordID]
		if !ok {
			return nil, resource.ErrRecordNotFound
		}

		switch v := item.(type) {
		// this can either be a map or a single entry
		case domain.Record:
			newRecord := make(domain.Record, len(v)+1)

			// A shallow copy is a better choice otherwise we'd be modifying the v under a Rlock() -  as of 1.21, maps.Copy replaces the need for a loop
			maps.Copy(newRecord, v)

			if _, hasID := newRecord.ID(); !hasID {
				newRecord.SetID(recordID)
			}
			return newRecord, nil

		default:
			record := domain.Record{
				"key":   recordID,
				"value": v,
			}

			record.SetID(recordID)
			return record, nil
		}
	default:
		// if we got this far, def not found
		return nil, resource.ErrWrongResourceType
	}
}

func (r *JsonRepository) CreateRecord(ctx context.Context, resourceName string, recordData domain.Record) (domain.Record, error) {
	if err := r.validateResourceName(resourceName); err != nil {
		return nil, err
	}

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
		normalisedRecord := r.normaliseLoadedValue(map[string]any(recordToStore))
		r.data[resourceName] = []any{normalisedRecord}

		if err := r.persist(); err != nil {

			// revert the change if saving to file fails
			delete(r.data, resourceName)
			return nil, err
		}
		return normalisedRecord.(domain.Record), nil
	}

	collection, err := r.asCollection(data)
	if err != nil {
		return nil, resource.ErrWrongResourceType
	}

	for _, item := range collection {

		if existingRecord, ok := item.(domain.Record); ok {

			if existingID, hasExistingID := existingRecord.ID(); hasExistingID && existingID == newID {
				return nil, resource.ErrDuplicateID
			}
		}
	}

	normalisedRecord := r.normaliseLoadedValue(map[string]any(recordToStore))
	newCollection := append(collection, normalisedRecord)

	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		r.data[resourceName] = collection

		log.Printf("Persistence failed, rolling back change for resource '%s': %v", resourceName, err)
		return nil, err
	}
	return normalisedRecord.(domain.Record), nil
}

// UpsertRecordByKey creates a new record or updates an existing one within a keyed-object resource, returning  the stored record, a boolean that is true if a new record was created (false if updated), and an error if the operation fails, such as when targeting a resource that is a collection.
// The operation is transactional; it updates the in-memory cache and persists the entire database to disk, rolling back the in-memory change if persistence fails.
// This method is safe for concurrent use.
func (r *JsonRepository) UpsertRecordByKey(ctx context.Context, resourceName, recordKey string, recordData domain.Record) (domain.Record, bool, error) {
	wasCreated := false

	if err := r.validateResourceName(resourceName); err != nil {
		return nil, wasCreated, err
	}

	if err := r.validateRecordKey(recordKey); err != nil {
		return nil, wasCreated, err
	}

	if err := recordData.Validate(); err != nil {
		return nil, wasCreated, fmt.Errorf("%w: %s", resource.ErrInvalidRecord, err.Error())
	}

	recordToStore := make(domain.Record, len(recordData))
	maps.Copy(recordToStore, recordData)
	delete(recordToStore, "id") // Remove potential ID field since keyed objects use the key as the identifier

	r.mu.Lock()
	defer r.mu.Unlock()

	originalResource, resourceExists := r.data[resourceName]

	var keyedObject domain.Record

	if resourceExists {
		var isMap bool
		keyedObject, isMap = originalResource.(domain.Record)

		// type assertion above fails so not a domain.Record (underlying map)
		if !isMap {
			return nil, wasCreated, resource.ErrWrongResourceType
		}
	} else {
		// the resource does not exist so create a new domain.Record for it
		keyedObject = make(domain.Record)
	}

	// we need this so that we return the correct wasCreated bool as it will determine returned http.StatusCode (201 vs 200) in the handler
	_, keyExisted := keyedObject[recordKey]
	wasCreated = !keyExisted

	// create a copy of the existing keyed object for modification (leaving the original unmodified)
	newKeyedObject := make(domain.Record, len(keyedObject)+1)
	maps.Copy(newKeyedObject, keyedObject)

	// normalise the new record
	normalisedRecord := r.normaliseLoadedValue(map[string]any(recordToStore))

	// add it to newKeyedObject we created
	newKeyedObject[recordKey] = normalisedRecord
	// modify the in-memory cache with the new/updated resource
	r.data[resourceName] = newKeyedObject

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		if !resourceExists {
			delete(r.data, resourceName)
		} else {
			r.data[resourceName] = originalResource
		}

		log.Printf("Persistence failed, rolling back change for resource '%s': %v", resourceName, err)
		return nil, wasCreated, err
	}

	record, ok := normalisedRecord.(domain.Record)
	if !ok {
		// This shouldn't happen, but better safe than sorry
		return nil, wasCreated, fmt.Errorf("internal error: failed to normalize record")
	}

	return record, wasCreated, nil
}

func (r *JsonRepository) DeleteRecordFromCollection(ctx context.Context, resourceName, recordID string) error {
	if err := r.validateResourceName(resourceName); err != nil {
		return err
	}

	if err := r.validateRecordID(recordID); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	data, ok := r.data[resourceName]
	if !ok {
		return resource.ErrResourceNotFound
	}

	collection, err := r.asCollection(data)
	if err != nil {
		return err
	}

	_, targetIndex := r.findRecordByID(collection, recordID)
	if targetIndex == -1 {
		return resource.ErrRecordNotFound
	}

	newCollection := append(collection[:targetIndex], collection[targetIndex+1:]...)
	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {

		r.data[resourceName] = collection

		log.Printf("Persistence failed, rolling back change for resource '%s': %v", resourceName, err)
		return err
	}

	return nil
}

func (r *JsonRepository) DeleteRecordByKey(ctx context.Context, resourceName, recordKey string) error {
	if err := r.validateResourceName(resourceName); err != nil {
		return err
	}

	if err := r.validateRecordKey(recordKey); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	originalResource, ok := r.data[resourceName]
	if !ok {
		return resource.ErrResourceNotFound
	}

	keyedObject, err := r.asKeyedObject(originalResource)
	if err != nil {
		return err
	}

	if _, keyExists := keyedObject[recordKey]; !keyExists {
		return resource.ErrRecordNotFound
	}

	newKeyedObject := make(domain.Record, len(keyedObject))
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

func (r *JsonRepository) UpdateRecordInCollection(ctx context.Context, resourceName, recordID string, recordData domain.Record) (domain.Record, error) {
	if err := r.validateResourceName(resourceName); err != nil {
		return nil, err
	}

	if err := r.validateRecordID(recordID); err != nil {
		return nil, err
	}

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
		return nil, resource.ErrResourceNotFound
	}

	collection, err := r.asCollection(data)
	if err != nil {
		return nil, err
	}

	_, recordPos := r.findRecordByID(collection, recordID)
	if recordPos == -1 {
		return nil, resource.ErrRecordNotFound
	}

	newCollection := make([]any, len(collection))
	copy(newCollection, collection)

	// Convert domain.Record to map[string]any before normalizing
	newCollection[recordPos] = r.normaliseLoadedValue(map[string]any(recordToStore))

	r.data[resourceName] = newCollection

	if err := r.persist(); err != nil {
		// revert changes if not possible to save to file
		r.data[resourceName] = collection

		log.Printf("Persistence failed, rolling back change for resource '%s': %v", resourceName, err)
		return nil, err
	}
	return newCollection[recordPos].(domain.Record), nil
}

func (r *JsonRepository) ListResources(ctx context.Context) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	resourceNames := make([]string, 0, len(r.data))

	for resource := range r.data {
		resourceNames = append(resourceNames, resource)
	}

	// output becomes predictable
	sort.Strings(resourceNames)

	return resourceNames, nil
}
