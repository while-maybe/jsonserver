package jsonrepo

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"math"
	"path"
	"path/filepath"
	"sort"
	"strings"

	jsonv2 "encoding/json/v2"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"jsonserver/internal/demodata"
	"jsonserver/internal/pkg/copier"
	"os"
	"sync"
)

type JsonRepository struct {
	p       Persister // Exported for testing
	w       Watcher
	mu      sync.RWMutex
	data    map[string]any // in-memory cache
	dataDir string
}

type Option func(*JsonRepository) error

const defaultDirPermissions = os.FileMode(0755)

var _ resource.Repository = (*JsonRepository)(nil)

func WithPersister(p Persister) Option {
	return func(r *JsonRepository) error {
		r.p = p
		return nil
	}
}

func WithWatcher(w Watcher) Option {
	return func(r *JsonRepository) error {
		r.w = w
		return nil
	}
}

func NewJsonRepository(dataDir string, opts ...Option) (*JsonRepository, error) {
	repo := &JsonRepository{
		data:    make(map[string]any),
		dataDir: dataDir,
	}

	if err := initDataDir(dataDir); err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	// apply provided options
	for _, opt := range opts {

		if err := opt(repo); err != nil {
			return nil, err
		}
	}

	// set persister and watcher if these were not in opts - default options
	if repo.p == nil {
		repo.p = NewFilePersister(dataDir)
	}

	if repo.w == nil {
		watcher, err := NewFsnotifyWatcher(dataDir, repo.loadFromDir)

		if err != nil {
			return nil, fmt.Errorf("failed to create file watcher: %w", err)
		}
		repo.w = watcher
	}

	if err := repo.loadFromDir(); err != nil {
		if os.IsNotExist(err) {

			log.Printf("WARN: Data directory '%s' not found. Starting with an empty repository.", dataDir)

			if err := os.MkdirAll(dataDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create data directory '%s': %w", dataDir, err)
			}

		} else {
			// Any other error during the initial load is a problem.
			return nil, fmt.Errorf("failed to perform initial data load: %w", err)
		}
	}
	log.Printf("Successfully loaded initial data from '%s'.", dataDir)

	return repo, nil
}

func (r *JsonRepository) loadFromDir() error {
	files, err := os.ReadDir(r.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	newData := make(map[string]any)

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		resourceName := strings.TrimSuffix(file.Name(), ".json")
		filePath := path.Join(r.dataDir, file.Name())

		bytes, err := os.ReadFile(filePath)
		// error reading from file
		if err != nil {
			log.Printf("WARN: failed to read resource file %s: %v", filePath, err)
			continue
		}
		// check for empty file
		if len(bytes) == 0 {
			log.Printf("WARN: resource file %s is empty", filePath)
			continue
		}

		var value any
		if err := jsonv2.Unmarshal(bytes, &value); err != nil {
			log.Printf("WARN: failed to unmarshal resource file %s: %v", filePath, err)
			continue
		}

		// Normalize and store
		newData[resourceName] = r.normaliseLoadedValue(value)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = newData
	log.Printf("Data reload complete. Loaded %d resources.", len(newData))

	return nil
}

// initDataDir is an unexported helper that ensures the data folder exists. If the folder does not exist, it creates it and populates it with embedded demo data.
func initDataDir(dataDir string) error {
	_, err := os.Stat(dataDir)

	if err != nil {

		if os.IsNotExist(err) {
			log.Printf("INFO: Data folder '%s' not found. Creating it with demo data.", dataDir)

			if err := os.MkdirAll(dataDir, defaultDirPermissions); err != nil {
				return fmt.Errorf("failed to create data folder: %w", err)
			}

			if err := writeDemoData(dataDir); err != nil {
				return fmt.Errorf("failed to write demo data: %w", err)
			}
		}
		// handle others like permission denied, etc
		return fmt.Errorf("failed to check data folder: %w", err)
	}

	// folder exists
	return nil
}

// writeDemoData walks the embedded filesystem and writes each demo file to disk.
func writeDemoData(destDir string) error {
	// must match what we specified in embedFS
	const sourceDir = "internal/_demo/data"

	// I like to write a literal so the call to WalkDir is not long
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		// if an error has been passed
		if err != nil {
			return err
		}

		// here we skip folders
		if d.Type().IsDir() {
			return nil
		}

		// otherwise attempt to read from embed...
		bytes, err := demodata.DemoDataFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read embedded file %s: %w", path, err)
		}

		destPath := filepath.Join(destDir, filepath.Base(path))

		// ...and write to disk - in the dest path
		return os.WriteFile(destPath, bytes, defaultFilePermissions)
	}

	// single line call to WalkDir thanks to walkFunc
	return fs.WalkDir(demodata.DemoDataFS, sourceDir, walkFunc)
}

func (r *JsonRepository) Watch(ctx context.Context) {
	// after injecting the dependency we simply call the .Watch() method from the dependency in r
	r.w.Watch(ctx)
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
			newRecord, err := copier.DeepCopy(v)
			if err != nil {
				return nil, err
			}

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

	recordToStore, err := copier.DeepCopy(recordData)
	if err != nil {
		return nil, err
	}

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

	recordToStore, err := copier.DeepCopy(recordData)
	if err != nil {
		return nil, wasCreated, err
	}
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
	// newKeyedObject := make(domain.Record, len(keyedObject)+1)
	// maps.Copy(newKeyedObject, keyedObject)

	newKeyedObject, err := copier.DeepCopy(keyedObject)
	if err != nil {
		return nil, wasCreated, err
	}

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

	newCollection := make([]any, 0, len(collection)-1)
	newCollection = append(newCollection, collection[:targetIndex]...)
	newCollection = append(newCollection, collection[targetIndex+1:]...)

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

	newKeyedObject, err := copier.DeepCopy(keyedObject)
	if err != nil {
		return err
	}

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

	recordToStore, err := copier.DeepCopy(recordData)
	if err != nil {
		return nil, err
	}

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

	newCollection, err := copier.DeepCopy(collection)
	if err != nil {
		return nil, err
	}

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
