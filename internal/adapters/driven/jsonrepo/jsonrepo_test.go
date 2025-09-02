package jsonrepo_test

import (
	"context"
	"jsonserver/internal/adapters/driven/jsonrepo"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testData = `{
	"students": {
		"Amy": 20,
		"David": 25
	},
	"buildings": [
		{"id": 5, "name": "lab"},
		{"id": 10, "name": "reception"},
		{"id": 25, "name": "classroom"}
	],
	"secret_code": 101
}`

func setupTestEnvironment(t *testing.T, initialData string) resource.Repository {
	tempDir := t.TempDir()
	tempDBFilename := filepath.Join(tempDir, "test_db.json")

	err := os.WriteFile(tempDBFilename, []byte(initialData), 0644)
	if err != nil {
		t.Fatalf("Failed to write initial test data: %v", err)
	}

	repo, err := jsonrepo.NewJsonRepository(tempDBFilename)
	if err != nil {
		log.Fatalf("Failed to initialize repository: %v", err)
	}
	return repo
}

func TestGetAllRecords(t *testing.T) {
	testCases := map[string]struct {
		resourceName string
		initialData  string
		wantRecords  []domain.Record
		wantErr      error
	}{
		"ok - collection": {
			resourceName: "buildings",
			initialData:  testData,
			wantRecords: []domain.Record{
				{"id": 5, "name": "lab"},
				{"id": 10, "name": "reception"},
				{"id": 25, "name": "classroom"},
			},
			wantErr: nil,
		},
		"ok - keyed object": {
			resourceName: "students",
			initialData:  testData,
			wantRecords: []domain.Record{
				{"key": "Amy", "value": 20},
				{"key": "David", "value": 25},
			},
			wantErr: nil,
		},
		"ok - single object": {
			resourceName: "secret_code",
			initialData:  testData,
			wantRecords: []domain.Record{
				{"key": "secret_code", "value": 101},
			},
			wantErr: nil,
		},
		"error - ErrEmptyResourceName": {
			resourceName: "",
			initialData:  testData,
			wantRecords: []domain.Record{
				{"id": 5, "name": "lab"},
				{"id": 10, "name": "reception"},
				{"id": 25, "name": "classroom"},
			},
			wantErr: resource.ErrEmptyResourceName,
		},
		"no data provided": {
			resourceName: "buildings",
			initialData:  "",
			wantRecords:  []domain.Record{},
			wantErr:      nil,
		},
		"non-existent resource": {
			resourceName: "missing_resource",
			initialData:  testData,
			wantRecords:  []domain.Record{},
			wantErr:      nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			records, err := repo.GetAllRecords(ctx, tc.resourceName)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)

			} else {
				assert.NoError(t, err)
				assert.Len(t, records, len(tc.wantRecords))

				// remember maps are unordered
				assert.ElementsMatch(t, tc.wantRecords, records)
			}
		})
	}
}

func TestGetRecordByID(t *testing.T) {
	testCases := map[string]struct {
		resourceName string
		recordID     string
		initialData  string
		wantRecord   domain.Record
		wantErr      error
	}{
		"ok - record in collection": {
			resourceName: "buildings",
			recordID:     "5",
			initialData:  testData,
			wantRecord:   domain.Record{"id": 5, "name": "lab"},
			wantErr:      nil,
		},
		"ok - record in keyed object": {
			resourceName: "students",
			recordID:     "Amy",
			initialData:  testData,
			wantRecord:   domain.Record{"id": "Amy", "key": "Amy", "value": 20},
			wantErr:      nil,
		},
		"error - record not found in collection": {
			resourceName: "students",
			recordID:     "NonExistentStudent",
			initialData:  testData,
			wantRecord:   nil,
			wantErr:      resource.ErrRecordNotFound,
		},
		"error - resource not found": {
			resourceName: "NonExistentResource",
			recordID:     "AnythingCanGoHere",
			initialData:  testData,
			wantRecord:   nil,
			wantErr:      resource.ErrResourceNotFound,
		},
		"error - wrong resource type": {
			resourceName: "secret_code",
			recordID:     "any-id",
			initialData:  testData,
			wantRecord:   nil,
			wantErr:      resource.ErrWrongResourceType,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			record, err := repo.GetRecordByID(ctx, tc.resourceName, tc.recordID)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)

			} else {
				assert.NoError(t, err)
				assert.Len(t, record, len(tc.wantRecord))

				// remember maps are unordered
				assert.Equal(t, tc.wantRecord, record)
			}
		})
	}
}
