package jsonrepo

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"maps"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	jsonv2 "encoding/json/v2"

	"jsonserver/internal/config"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"jsonserver/internal/demodata"
	"jsonserver/internal/pkg/copier"
	"os"
	"sync"
)

type JsonRepository struct {
	p            Persister // Exported for testing
	w            Watcher
	mu           sync.RWMutex
	data         map[string]any // in-memory cache
	dataDir      string
	normaliser   *dataNormaliser
	isPersisting atomic.Bool
	dirty        map[string]bool // resources that have changed since last load from file
	// persistence fields
	mode             config.PersistenceMode
	persistenceTimer time.Duration
	writeQueue       chan struct{} // signals there is something waiting to be written
	shutdown         chan struct{} // signals background processes to stop
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

func NewJsonRepository(cfg *config.Config, opts ...Option) (*JsonRepository, error) {
	repo := &JsonRepository{
		data:       make(map[string]any),
		dataDir:    cfg.DataDir,
		normaliser: NewDataNormaliser(),
		dirty:      make(map[string]bool),
		// set the persistence mode from the config
		mode:             cfg.PersistenceMode,
		persistenceTimer: cfg.PersistenceTimer,
		writeQueue:       make(chan struct{}, 1),
		shutdown:         make(chan struct{}),
	}

	// apply provided options
	for _, opt := range opts {

		if err := opt(repo); err != nil {
			return nil, err
		}
	}

	// set persister and watcher if these were not in opts - default options
	if repo.p == nil {
		repo.p = NewFilePersister(repo.dataDir)
	}

	if repo.w == nil {

		isInternalChangeCheck := func() bool {
			return repo.isPersisting.Load()
		}

		watcher, err := NewFsnotifyWatcher(repo.dataDir, repo.loadFromDir, isInternalChangeCheck)

		if err != nil {
			return nil, fmt.Errorf("failed to create file watcher: %w", err)
		}
		repo.w = watcher
	}

	if err := repo.initialise(); err != nil {
		return nil, fmt.Errorf("failed to initialise data directory: %w", err)
	}

	if repo.mode == config.ModeBatched {
		go repo.batchPersistWorker(repo.persistenceTimer)
	}

	// if err := repo.loadFromDir(); err != nil {
	// 	if os.IsNotExist(err) {

	// 		log.Printf("WARN: Data directory '%s' not found. Starting with an empty repository.", repo.dataDir)

	// 		if err := os.MkdirAll(repo.dataDir, defaultDirPermissions); err != nil {
	// 			return nil, fmt.Errorf("failed to create data directory '%s': %w", repo.dataDir, err)
	// 		}

	// 	} else {
	// 		// Any other error during the initial load is a problem.
	// 		return nil, fmt.Errorf("failed to perform initial data load: %w", err)
	// 	}
	// }
	log.Printf("Successfully loaded initial data from '%s'.", repo.dataDir)

	return repo, nil
}

// initialize handles the entire startup process: ensuring the directory exists, populating it with demo data if needed, and performing the initial load.
func (r *JsonRepository) initialise() error {
	info, err := os.Stat(r.dataDir)

	if err != nil {

		if os.IsNotExist(err) {
			log.Printf("INFO: Data folder '%s' not found. Creating it with demo data.", r.dataDir)

			if err := os.MkdirAll(r.dataDir, defaultDirPermissions); err != nil {
				return fmt.Errorf("failed to create data folder: %w", err)
			}

			if err := writeDemoData(r.dataDir); err != nil {
				return fmt.Errorf("failed to write demo data: %w", err)
			}

			// we created the demo data
			return nil
		}
		// handle other errors like permission denied, etc
		return fmt.Errorf("failed to check data folder: %w", err)

	} else if !info.IsDir() {
		return fmt.Errorf("data path '%s' exists but is a file, not a directory", r.dataDir)
	}

	// folder exists
	return r.loadFromDir()
}

func (r *JsonRepository) batchPersistWorker(saveInterval time.Duration) {
	ticker := time.NewTicker(saveInterval)
	defer ticker.Stop()

	log.Printf("INFO: Batch persistence worker started. Will persist every %s.", saveInterval)

	for {
		select {
		case <-ticker.C:
			if err := r.persist(); err != nil {
				log.Printf("ERROR: Background batched persistence failed: %v", err)
			}
		case <-r.shutdown:
			log.Println("INFO: Shutdown signal received, performing final flush.")
			if err := r.persist(); err != nil {
				log.Printf("ERROR: Final flush on shutdown failed: %v", err)
			}
			return
		}
	}
}

// NewRepositoryFromData creates a new repository initialised with a pre-existing map of data. It uses an in-memory-only persister and no-op watcher.
func NewJsonRepositoryFromData(initialData map[string]any) resource.Repository {
	normalisedData := make(map[string]any)
	normaliser := NewDataNormaliser()

	for k, v := range initialData {
		normalisedData[k] = normaliser.normalise(v)
	}

	repo := &JsonRepository{
		p:          NewNoOpPersister(),
		w:          NewNoOpWatcher(),
		data:       normalisedData,
		normaliser: normaliser,
		dirty:      make(map[string]bool),
	}

	return repo
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
		newData[resourceName] = r.normaliser.normalise(value)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = newData
	log.Printf("Data reload complete. Loaded %d resources.", len(newData))

	return nil
}

// we placed this functionality inside initialise above
// initDataDir is an unexported helper that ensures the data folder exists. If the folder does not exist, it creates it and populates it with embedded demo data.
// func initDataDir(dataDir string) error {
// 	_, err := os.Stat(dataDir)

// 	if err != nil {

// 		if os.IsNotExist(err) {
// 			log.Printf("INFO: Data folder '%s' not found. Creating it with demo data.", dataDir)

// 			if err := os.MkdirAll(dataDir, defaultDirPermissions); err != nil {
// 				return fmt.Errorf("failed to create data folder: %w", err)
// 			}

// 			if err := writeDemoData(dataDir); err != nil {
// 				return fmt.Errorf("failed to write demo data: %w", err)
// 			}

// 			// we created the demo data
// 			return nil
// 		}
// 		// handle other errors like permission denied, etc
// 		return fmt.Errorf("failed to check data folder: %w", err)
// 	}

// 	// folder exists
// 	return nil
// }

// writeDemoData walks the embedded filesystem and writes each demo file to disk.
func writeDemoData(destDir string) error {
	// must match what we specified in embedFS
	const sourceDir = "."

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

// persist writes the in-memory dirty resourceNames to the corresponding json file equivalent
func (r *JsonRepository) persist() error {
	if len(r.dirty) == 0 {
		return nil
	}

	r.isPersisting.Store(true)
	defer r.isPersisting.Store(false)

	r.mu.RLock()

	// capture dirty resources
	dirtyToPersist := make(map[string]bool, len(r.dirty))
	maps.Copy(dirtyToPersist, r.dirty)

	// capture data for dirty resources while we have the lock
	dataSnapshot := make(map[string]interface{}, len(dirtyToPersist))
	for resourceName := range dirtyToPersist {
		if resourceData, ok := r.data[resourceName]; ok {
			dataSnapshot[resourceName] = resourceData
		}
	}

	// capture resources for cleanup
	activeResources := make(map[string]bool, len(r.data))
	for name := range r.data {
		activeResources[name] = true
	}
	r.mu.RUnlock()

	log.Printf("Persisting %d dirty resources...", len(dirtyToPersist))

	// track which resources were successfully persisted
	persistedResources := make(map[string]bool)

	// persist the dirty resources
	for resourceName := range dirtyToPersist {
		resourceData, ok := dataSnapshot[resourceName]

		// the resource was deleted so we don't need it but should still be marked for cleanup
		if !ok {
			persistedResources[resourceName] = true
			continue
		}

		denormalisedData := r.normaliser.denormalise(resourceData)
		if err := r.p.PersistOne(resourceName, denormalisedData); err != nil {
			log.Printf("ERROR: failed to persist resource %s: %v", resourceName, err)
			return fmt.Errorf("failed to persist resource %s: %w", resourceName, err)
		}

		persistedResources[resourceName] = true
	}

	// after all writes we can clean up
	if err := r.p.Cleanup(activeResources); err != nil {
		log.Printf("WARN: an error occurred during persistence cleanup: %v", err)
	}

	// clean only the dirty flags we wrote
	r.mu.Lock()
	for resourceName := range dirtyToPersist {
		delete(r.dirty, resourceName)
	}
	r.mu.Unlock()

	log.Printf("Successfully persisted %d resources", len(persistedResources))
	return nil
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

// handleWrite is the central method for managing persistence after an in-memory write operation.
func (r *JsonRepository) handleWrite() error {
	switch r.mode {

	case config.ModeImmediateSync:
		return r.persist()

	case config.ModeImmediateAsync:
		go func() {
			if err := r.persist(); err != nil {
				log.Printf("ERROR: Async persistence failed: %v", err)
			}
		}()

	case config.ModeBatched:
		// the background worker will do this
	}

	return nil
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
	normalisedRecord, ok := r.normaliser.normalise(map[string]any(recordToStore)).(domain.Record)
	if !ok {
		return nil, fmt.Errorf("could not normalise recordData")
	}

	var rollbackFunc func()

	r.mu.Lock()

	originalData, hasData := r.data[resourceName]

	if !hasData {
		// new resource
		r.data[resourceName] = []any{normalisedRecord}
		r.dirty[resourceName] = true

		rollbackFunc = func() {
			delete(r.data, resourceName)
			delete(r.dirty, resourceName)
		}
	} else {
		// existing resource
		collection, err := r.asCollection(originalData)
		if err != nil {
			return nil, resource.ErrWrongResourceType
		}

		// check for duplicate IDs
		for _, item := range collection {

			if existingRecord, ok := item.(domain.Record); ok {

				if existingID, hasExistingID := existingRecord.ID(); hasExistingID && existingID == newID {
					return nil, resource.ErrDuplicateID
				}
			}
		}

		// this is safer than append(collection, normalisedRecord)
		newCollection := make([]any, 0, len(collection)+1)
		newCollection = append(newCollection, collection...)
		newCollection = append(newCollection, normalisedRecord)

		r.data[resourceName] = newCollection
		r.dirty[resourceName] = true

		rollbackFunc = func() {
			r.data[resourceName] = originalData
			delete(r.dirty, resourceName)
		}
	}

	r.mu.Unlock()

	// writing outside the lock
	if err := r.handleWrite(); err != nil {
		log.Printf("Persistence failed, rolling back CreateRecord for resource '%s'", resourceName)

		r.mu.Lock()
		rollbackFunc()
		r.mu.Unlock()

		return nil, err
	}

	return normalisedRecord, nil
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

	// normalise the new record
	normalisedRecord, ok := r.normaliser.normalise(map[string]any(recordToStore)).(domain.Record)
	if !ok {
		// This shouldn't happen, but better safe than sorry
		return nil, wasCreated, fmt.Errorf("internal error: failed to normalize record")
	}

	var rollbackFunc func()

	r.mu.Lock()

	originalResource, resourceExists := r.data[resourceName]

	var keyedObject domain.Record

	if resourceExists {
		var isMap bool
		keyedObject, isMap = originalResource.(domain.Record)

		// type assertion above fails so not a domain.Record (underlying map)
		if !isMap {
			r.mu.Unlock()
			return nil, wasCreated, resource.ErrWrongResourceType
		}

		rollbackFunc = func() {
			r.data[resourceName] = originalResource
			delete(r.dirty, resourceName)
		}
	} else {
		// the resource does not exist so create a new domain.Record for it
		keyedObject = make(domain.Record)

		rollbackFunc = func() {
			delete(r.data, resourceName)
			delete(r.dirty, resourceName)
		}
	}

	// we need this so that we return the correct wasCreated bool as it will determine returned http.StatusCode (201 vs 200) in the handler
	_, keyExisted := keyedObject[recordKey]
	wasCreated = !keyExisted

	newKeyedObject, err := copier.DeepCopy(keyedObject)
	if err != nil {
		r.mu.Unlock()
		return nil, wasCreated, err
	}

	// add it to newKeyedObject we created
	newKeyedObject[recordKey] = normalisedRecord

	// modify the in-memory cache with the new/updated resource
	r.data[resourceName] = newKeyedObject
	r.dirty[resourceName] = true

	r.mu.Unlock()

	if err := r.handleWrite(); err != nil {
		// revert changes if not possible to save to file
		r.mu.Lock()
		rollbackFunc()
		r.mu.Unlock()

		log.Printf("Persistence failed, rolling back change for resource '%s': %v", resourceName, err)
		return nil, wasCreated, err
	}

	return normalisedRecord, wasCreated, nil
}

func (r *JsonRepository) DeleteRecordFromCollection(ctx context.Context, resourceName, recordID string) error {
	if err := r.validateResourceName(resourceName); err != nil {
		return err
	}

	if err := r.validateRecordID(recordID); err != nil {
		return err
	}

	r.mu.Lock()

	data, ok := r.data[resourceName]
	if !ok {
		r.mu.Unlock() // needed in case of early return
		return resource.ErrResourceNotFound
	}

	collection, err := r.asCollection(data)
	if err != nil {
		r.mu.Unlock() // needed in case of early return
		return err
	}

	_, targetIndex := r.findRecordByID(collection, recordID)
	if targetIndex == -1 {
		r.mu.Unlock() // needed in case of early return
		return resource.ErrRecordNotFound
	}

	newCollection := make([]any, 0, len(collection)-1)
	newCollection = append(newCollection, collection[:targetIndex]...)
	newCollection = append(newCollection, collection[targetIndex+1:]...)

	r.data[resourceName] = newCollection
	r.dirty[resourceName] = true

	r.mu.Unlock()

	if err := r.handleWrite(); err != nil {

		r.mu.Lock()

		r.data[resourceName] = collection
		delete(r.dirty, resourceName)

		r.mu.Unlock()

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

	originalResource, ok := r.data[resourceName]
	if !ok {
		r.mu.Unlock()
		return resource.ErrResourceNotFound
	}

	keyedObject, err := r.asKeyedObject(originalResource)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	if _, keyExists := keyedObject[recordKey]; !keyExists {
		r.mu.Unlock()
		return resource.ErrRecordNotFound
	}

	newKeyedObject, err := copier.DeepCopy(keyedObject)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	delete(newKeyedObject, recordKey)

	r.data[resourceName] = newKeyedObject
	r.dirty[resourceName] = true

	r.mu.Unlock()

	if err := r.handleWrite(); err != nil {
		// revert changes if not possible to save to file
		r.mu.Lock()
		r.data[resourceName] = originalResource
		delete(r.dirty, resourceName)
		r.mu.Unlock()

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

	normalisedRecord := r.normaliser.normalise(map[string]any(recordToStore))

	r.mu.Lock()

	data, hasData := r.data[resourceName]

	if !hasData {
		r.mu.Unlock()
		return nil, resource.ErrResourceNotFound
	}

	collection, err := r.asCollection(data)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}

	_, recordPos := r.findRecordByID(collection, recordID)
	if recordPos == -1 {
		r.mu.Unlock()
		return nil, resource.ErrRecordNotFound
	}

	newCollection, err := copier.DeepCopy(collection)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}

	newCollection[recordPos] = normalisedRecord

	originalCollection := collection

	r.data[resourceName] = newCollection
	r.dirty[resourceName] = true

	r.mu.Unlock()

	if err := r.handleWrite(); err != nil {
		log.Printf("Persistence failed, rolling back UpdateRecordInCollection for resource '%s': %v", resourceName, err)

		r.mu.Lock()
		// revert changes if not possible to save to file
		r.data[resourceName] = originalCollection
		delete(r.dirty, resourceName)
		r.mu.Unlock()

		log.Printf("Persistence failed, rolling back change for resource '%s': %v", resourceName, err)
		return nil, err
	}

	// Return the normalized record (type assertion should be safe here)
	updatedRecord, ok := normalisedRecord.(domain.Record)
	if !ok {
		log.Printf("WARN: normalised record is not domain.Record type: %T", normalisedRecord)

		if storedRecord, ok := newCollection[recordPos].(domain.Record); ok {
			return storedRecord, nil
		}

		return nil, fmt.Errorf("internal error: failed to return updated record")
	}
	return updatedRecord, nil
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
