package jsonrepo

import (
	"context"

	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"os"
	"sync"
)

// below is placed here so import doesn't complain

type jsonRepository struct {
	filename string
	mu       sync.RWMutex
	data     map[string][]domain.Record // in-memory cache
}

var _ resource.Repository = (*jsonRepository)(nil)

var (
	ErrResourceNotFound = errors.New("resource not found in repository")
	ErrRecordNotFound   = errors.New("record not found in memory")
)

func NewJsonRepository(filename string) (resource.Repository, error) {
	repo := &jsonRepository{
		filename: filename,
		data:     make(map[string][]domain.Record),
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
		return err
	}

	// check for empty file
	if len(bytes) == 0 {
		return nil
	}

	// TODO investigate if MarshalRead should replace all the below

	var rawData map[string][]domain.Record
	if err := jsonv2.Unmarshal(bytes, &rawData); err != nil {
		return err
	}

	r.data = rawData
	return nil
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

func (r *jsonRepository) GetAllRecords(ctx context.Context, resourceName string) ([]domain.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	data, hasData := r.data[resourceName]
	if !hasData {
		// non-existent resource is not an error
		return []domain.Record{}, nil
	}

	return data, nil
}

func (r *jsonRepository) GetRecordByID(ctx context.Context, resourceName, recordID string) (domain.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	records, ok := r.data[resourceName]
	if !ok {
		return nil, resource.ErrNotFound
	}

	for _, record := range records {

		if id, ok := record.ID(); ok && id == recordID {
			return record, nil
		}
	}

	return nil, resource.ErrNotFound
}

// TODO implement remaining methods

func (r *jsonRepository) CreateRecord(ctx context.Context, resourceName string, recordData domain.Record) (domain.Record, error) {
	return nil, nil
}

func (r *jsonRepository) UpsertRecordByKey(ctx context.Context, resourceName, recordKey string, recordData domain.Record) (domain.Record, error) {
	return nil, nil
}
