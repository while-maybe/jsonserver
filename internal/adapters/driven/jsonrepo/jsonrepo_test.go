package jsonrepo_test

import (
	"context"
	"jsonserver/internal/adapters/driven/jsonrepo"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"log"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testData = `{
	"students": {
		"Amy": 20,
		"David": 25
	},
	"buildings": [
		{"id": "5", "name": "lab"},
		{"id": "10", "name": "reception"},
		{"id": "25", "name": "classroom"}
	],
	"secret_code": 101
}`

func setupTestEnvironment(t *testing.T, initialData string) (resource.Repository, string) {
	t.Helper()

	tempDir := t.TempDir()
	dbFilename := filepath.Join(tempDir, "test_db.json")

	err := os.WriteFile(dbFilename, []byte(initialData), 0644)
	if err != nil {
		t.Fatalf("Failed to write initial test data: %v", err)
	}

	repo, err := jsonrepo.NewJsonRepository(dbFilename)
	if err != nil {
		log.Fatalf("Failed to initialize repository: %v", err)
	}
	return repo, dbFilename
}

// verifyResourceState is a test helper that checks both the cache and the persisted file
// to ensure the resource matches the expected state.
func verifyResourceState(t *testing.T, ctx context.Context, repo resource.Repository, dbFilename, resourceName string, wantState []domain.Record) {
	t.Helper()

	// assert equal record in-memory cache
	recordsInCache, err := repo.GetAllRecords(ctx, resourceName)
	require.NoError(t, err, "should be able to fetch all records")

	// assert in-memory cache wanted collection
	assert.Len(t, recordsInCache, len(wantState))
	assert.ElementsMatch(t, wantState, recordsInCache)

	// get a new repo from test file
	persistedRepo, err := jsonrepo.NewJsonRepository(dbFilename)
	require.NoError(t, err, "failed to create a new repo from the persisted file")

	recordsInFile, err := persistedRepo.GetAllRecords(ctx, resourceName)
	require.NoError(t, err, "should be able to fetch the persisted record from a new repo instance")

	// assert wanted collection in file
	assert.ElementsMatch(t, wantState, recordsInFile)
}

func TestGetAllRecords(t *testing.T) {
	testCases := map[string]struct {
		resourceName   string
		initialData    string
		wantCollection []domain.Record
		wantErr        error
	}{
		"ok - collection": {
			resourceName: "buildings",
			initialData:  testData,
			wantCollection: []domain.Record{
				{"id": "5", "name": "lab"},
				{"id": "10", "name": "reception"},
				{"id": "25", "name": "classroom"},
			},
			wantErr: nil,
		},
		"ok - keyed object": {
			resourceName: "students",
			initialData:  testData,
			wantCollection: []domain.Record{
				{"key": "Amy", "value": 20},
				{"key": "David", "value": 25},
			},
			wantErr: nil,
		},
		"ok - single object": {
			resourceName: "secret_code",
			initialData:  testData,
			wantCollection: []domain.Record{
				{"key": "secret_code", "value": 101},
			},
			wantErr: nil,
		},
		"error - ErrEmptyResourceName": {
			resourceName:   "",
			initialData:    testData,
			wantCollection: nil,
			wantErr:        resource.ErrEmptyResourceName,
		},
		"no data provided": {
			resourceName:   "buildings",
			initialData:    "",
			wantCollection: []domain.Record{},
			wantErr:        nil,
		},
		"non-existent resource": {
			resourceName:   "missing_resource",
			initialData:    testData,
			wantCollection: []domain.Record{},
			wantErr:        nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo, _ := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			recordsInCache, err := repo.GetAllRecords(ctx, tc.resourceName)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)

				// we can also assert that no data was return on error
				assert.Nil(t, recordsInCache, "should not return records on error")
				return
			}

			require.NoError(t, err)

			// assert in-memory cache wanted collection
			assert.Len(t, recordsInCache, len(tc.wantCollection))
			assert.ElementsMatch(t, tc.wantCollection, recordsInCache)
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
			wantRecord:   domain.Record{"id": "5", "name": "lab"},
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

			repo, _ := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			record, err := repo.GetRecordByID(ctx, tc.resourceName, tc.recordID)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return

			}
			assert.NoError(t, err)
			assert.Len(t, record, len(tc.wantRecord))

			assert.Equal(t, tc.wantRecord, record)
		})
	}
}

func TestCreateRecord(t *testing.T) {
	testCases := map[string]struct {
		resourceName       string
		initialData        string
		recordToAdd        domain.Record
		wantReturnedRecord domain.Record
		wantCollection     []domain.Record
		wantErr            error
	}{
		"ok - record in collection": {
			resourceName:       "buildings",
			initialData:        testData,
			recordToAdd:        domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantErr:            nil,
		},
		"ok - record in new collection": {
			resourceName:       "new_resource_name",
			initialData:        testData,
			recordToAdd:        domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantErr:            nil,
		},
		"error - resource.ErrEmptyResourceName": {
			resourceName:       "",
			initialData:        testData,
			recordToAdd:        domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantErr:            resource.ErrEmptyResourceName,
		},
		"error - resource.ErrInvalidRecord": {
			resourceName:       "buildings",
			initialData:        testData,
			recordToAdd:        domain.Record{"name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantErr:            resource.ErrInvalidRecord,
		},
		"error - resource.ErrWrongResourceType": {
			resourceName:       "students",
			initialData:        testData,
			recordToAdd:        domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantErr:            resource.ErrWrongResourceType,
		},
		"error - resource.ErrDuplicateID": {
			resourceName:       "buildings",
			initialData:        testData,
			recordToAdd:        domain.Record{"id": "25", "name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantErr:            resource.ErrDuplicateID,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo, dbFilename := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			createdRecord, err := repo.CreateRecord(ctx, tc.resourceName, tc.recordToAdd)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return

			}
			assert.NoError(t, err)

			// assert equal record is returned by CreateRecord
			assert.Equal(t, tc.wantReturnedRecord, createdRecord)

			// assert equal record in-memory cache
			recordID, _ := tc.recordToAdd.ID()
			inCacheRecord, err := repo.GetRecordByID(ctx, tc.resourceName, recordID)
			require.NoError(t, err, "should be able to fetch the newly created record")

			assert.Equal(t, createdRecord, inCacheRecord)

			// assert equal record in file
			persistedRepo, err := jsonrepo.NewJsonRepository(dbFilename)
			require.NoError(t, err, "failed to create a new repo from the persisted file")

			persistedRecord, err := persistedRepo.GetRecordByID(ctx, tc.resourceName, recordID)
			require.NoError(t, err, "Should be able to fetch the persisted record from a new repo instance")

			assert.Equal(t, createdRecord, persistedRecord)
		})
	}
}

func TestUpsertRecordByKey(t *testing.T) {
	testCases := map[string]struct {
		initialData        string
		resourceName       string
		recordKey          string
		recordToAdd        domain.Record
		wantReturnedRecord domain.Record
		wantNewRecord      bool
		wantErr            error
	}{
		"ok - insert new record in keyed object": {
			resourceName: "students",
			recordKey:    "Mary",
			initialData:  testData,
			// 31.0 can test float64 to int conversion
			recordToAdd:        domain.Record{"age": 31.0},
			wantReturnedRecord: domain.Record{"age": 31},
			wantNewRecord:      true,
			wantErr:            nil,
		}, "ok - update existing record in keyed object": {
			resourceName: "students",
			recordKey:    "Amy",
			initialData:  testData,
			// 31.0 can test float64 to int conversion
			recordToAdd:        domain.Record{"age": 10.0},
			wantReturnedRecord: domain.Record{"age": 10},
			wantNewRecord:      false,
			wantErr:            nil,
		},
		"error - resource.ErrInvalidRecord": {
			resourceName:       "students",
			recordKey:          "Amy",
			initialData:        testData,
			recordToAdd:        nil,
			wantReturnedRecord: nil,
			wantNewRecord:      false,
			wantErr:            resource.ErrInvalidRecord,
		},
		"error - resource.ErrEmptyRecordKey": {
			resourceName:       "students",
			recordKey:          "",
			initialData:        testData,
			recordToAdd:        domain.Record{"age": 10.0},
			wantReturnedRecord: domain.Record{"age": 10},
			wantNewRecord:      false,
			wantErr:            resource.ErrEmptyRecordKey,
		},
		"error - resource.ErrInvalidResourceName": {
			resourceName:       "",
			recordKey:          "Amy",
			initialData:        testData,
			recordToAdd:        domain.Record{"age": 10.0},
			wantReturnedRecord: domain.Record{"age": 10},
			wantNewRecord:      false,
			wantErr:            resource.ErrEmptyResourceName,
		},
		"error - resource.ErrWrongResourceType": {
			resourceName:       "buildings",
			recordKey:          "Amy",
			initialData:        testData,
			recordToAdd:        domain.Record{"age": 10.0},
			wantReturnedRecord: domain.Record{"age": 10},
			wantNewRecord:      false,
			wantErr:            resource.ErrWrongResourceType,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo, dbFilename := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			createdRecord, isNewRecord, err := repo.UpsertRecordByKey(ctx, tc.resourceName, tc.recordKey, tc.recordToAdd)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return

			}
			assert.NoError(t, err)

			// assert is record is newly created or updated
			assert.Equal(t, tc.wantNewRecord, isNewRecord)

			// assert equal record is returned by CreateRecord
			assert.Equal(t, tc.wantReturnedRecord, createdRecord)

			// assert equal record in-memory cache
			inCacheRecord, err := repo.GetRecordByID(ctx, tc.resourceName, tc.recordKey)
			require.NoError(t, err, "should be able to fetch the newly created record")

			// simply doing inCacheRecord["id"] = tc.recordKey would go against a testing principal which dictates to never alter the outcome of what is being tested - in this case inCacheRecord should not be modified in any way
			// this also means doing delete(inCacheRecord, "id") above should never be done as it also modifies data to be tested
			expectedFetchedRecord := make(domain.Record)
			maps.Copy(expectedFetchedRecord, tc.wantReturnedRecord)
			expectedFetchedRecord["id"] = tc.recordKey

			assert.Equal(t, expectedFetchedRecord, inCacheRecord)

			// assert equal record in file
			persistedRepo, err := jsonrepo.NewJsonRepository(dbFilename)
			require.NoError(t, err, "failed to create a new repo from the persisted file")

			persistedRecord, err := persistedRepo.GetRecordByID(ctx, tc.resourceName, tc.recordKey)
			require.NoError(t, err, "should be able to fetch the persisted record from a new repo instance")

			assert.Equal(t, expectedFetchedRecord, persistedRecord)
		})
	}
}

func TestDeleteRecordFromCollection(t *testing.T) {
	testCases := map[string]struct {
		initialData       string
		resourceName      string
		recordKeytoDelete string
		wantCollection    []domain.Record
		wantErr           error
	}{
		"ok - delete a record from a collection": {
			initialData:       testData,
			resourceName:      "buildings",
			recordKeytoDelete: "5",
			wantCollection:    []domain.Record{{"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:           nil,
		},
		"ok - delete the last record from a collection": {
			initialData:       `{"buildings": [{"id": "5", "name": "lab"}]}`,
			resourceName:      "buildings",
			recordKeytoDelete: "5",
			wantCollection:    []domain.Record{},
			wantErr:           nil,
		},
		"error - resource.ErrEmptyResourceName": {
			initialData:       testData,
			resourceName:      "",
			recordKeytoDelete: "5",
			wantCollection:    nil,
			wantErr:           resource.ErrEmptyResourceName,
		},
		"error - resource.ErrEmptyRecordID": {
			initialData:       testData,
			resourceName:      "buildings",
			recordKeytoDelete: "",
			wantCollection:    []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:           resource.ErrEmptyRecordID,
		},
		"error - resource.ErrResourceNotFound": {
			initialData:       testData,
			resourceName:      "doesNotExist",
			recordKeytoDelete: "5",
			wantCollection:    nil,
			wantErr:           resource.ErrResourceNotFound,
		},
		"error - resource.ErrRecordNotFound": {
			initialData:       testData,
			resourceName:      "buildings",
			recordKeytoDelete: "1000",
			wantCollection:    []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:           resource.ErrRecordNotFound,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo, dbFilename := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			err := repo.DeleteRecordFromCollection(ctx, tc.resourceName, tc.recordKeytoDelete)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}

			// check if data is the expected after operation fails
			if tc.wantCollection == nil {
				return
			}

			verifyResourceState(t, ctx, repo, dbFilename, tc.resourceName, tc.wantCollection)
		})
	}
}

func TestDeleteRecordByKey(t *testing.T) {
	testCases := map[string]struct {
		initialData       string
		resourceName      string
		recordKeytoDelete string
		wantKeyedObject   []domain.Record
		wantErr           error
	}{
		"ok - delete a record from a keyed object": {
			initialData:       testData,
			resourceName:      "students",
			recordKeytoDelete: "Amy",
			wantKeyedObject:   []domain.Record{{"key": "David", "value": 25}},
			wantErr:           nil,
		},
		"ok - delete the last record from a keyed object": {
			initialData:       `{"students": {"Amy": 20}}`,
			resourceName:      "students",
			recordKeytoDelete: "Amy",
			wantKeyedObject:   []domain.Record{},
			wantErr:           nil,
		},
		"error - resource.ErrEmptyResourceName": {
			initialData:       testData,
			resourceName:      "",
			recordKeytoDelete: "Amy",
			wantKeyedObject:   nil,
			wantErr:           resource.ErrEmptyResourceName,
		},
		"error - resource.ErrEmptyRecordKey": {
			initialData:       testData,
			resourceName:      "students",
			recordKeytoDelete: "",
			wantKeyedObject:   nil,
			wantErr:           resource.ErrEmptyRecordKey,
		},
		"error - resource.ErrResourceNotFound": {
			initialData:       testData,
			resourceName:      "doesNotExist", // this resource does not exist
			recordKeytoDelete: "Amy",
			wantKeyedObject:   nil,
			wantErr:           resource.ErrResourceNotFound,
		},
		"error - resource.ErrRecordNotFound": {
			initialData:       testData,
			resourceName:      "students",
			recordKeytoDelete: "Jonathan", // this key is not in the map
			wantKeyedObject:   []domain.Record{{"key": "Amy", "value": 20}, {"key": "David", "value": 25}},
			wantErr:           resource.ErrRecordNotFound,
		},
		"error - resource.ErrWrongResourceType": {
			initialData:       testData,
			resourceName:      "buildings",
			recordKeytoDelete: "Amy",
			wantKeyedObject:   nil,
			wantErr:           resource.ErrWrongResourceType,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo, dbFilename := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			err := repo.DeleteRecordByKey(ctx, tc.resourceName, tc.recordKeytoDelete)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}

			// check if data is the expected after operation fails
			if tc.wantKeyedObject == nil {
				return
			}

			verifyResourceState(t, ctx, repo, dbFilename, tc.resourceName, tc.wantKeyedObject)
		})
	}
}

func TestUpdateRecordInCollection(t *testing.T) {
	testCases := map[string]struct {
		resourceName       string
		initialData        string
		recordID           string
		recordToUpdate     domain.Record
		wantReturnedRecord domain.Record
		wantCollection     []domain.Record
		wantErr            error
	}{
		"ok - update record in collection": {
			resourceName:       "buildings",
			initialData:        testData,
			recordID:           "25",
			recordToUpdate:     domain.Record{"id": "25", "name": "shop", "floors": 2},
			wantReturnedRecord: domain.Record{"id": "25", "name": "shop", "floors": 2},
			wantCollection:     []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "shop", "floors": 2}},
			wantErr:            nil,
		},
		"error - resource.ErrEmptyResourceName": {
			resourceName:       "",
			initialData:        testData,
			recordID:           "25",
			recordToUpdate:     domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     nil,
			wantErr:            resource.ErrEmptyResourceName,
		},
		"error - resource.ErrEmptyRecordID": {
			resourceName:       "buildings",
			initialData:        testData,
			recordID:           "",
			recordToUpdate:     domain.Record{"id": "25", "name": "shop", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:            resource.ErrEmptyRecordID,
		},
		"error - resource.ErrInvalidRecord": {
			resourceName:       "buildings",
			initialData:        testData,
			recordID:           "25",
			recordToUpdate:     domain.Record{"id": 25, "name": "shop", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:            resource.ErrInvalidRecord,
		},
		"error - resource.ErrMismatchedID": {
			resourceName:       "buildings",
			initialData:        testData,
			recordID:           "999",
			recordToUpdate:     domain.Record{"id": "25", "name": "shop", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:            resource.ErrMismatchedID,
		},
		"error - resource.ErrWrongResourceType": {
			resourceName:       "students",
			initialData:        testData,
			recordID:           "50",
			recordToUpdate:     domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     []domain.Record{{"key": "Amy", "value": 20}, {"key": "David", "value": 25}},
			wantErr:            resource.ErrWrongResourceType,
		},
		"error - resource.ErrResourceNotFound": {
			resourceName:       "doesNotExist",
			initialData:        testData,
			recordID:           "50",
			recordToUpdate:     domain.Record{"id": "50", "name": "garage", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     nil,
			wantErr:            resource.ErrResourceNotFound,
		},
		"error - resource.ErrRecordNotFound": {
			resourceName:       "buildings",
			initialData:        testData,
			recordID:           "999",
			recordToUpdate:     domain.Record{"id": "999", "name": "shop", "floors": 2},
			wantReturnedRecord: nil,
			wantCollection:     []domain.Record{{"id": "5", "name": "lab"}, {"id": "10", "name": "reception"}, {"id": "25", "name": "classroom"}},
			wantErr:            resource.ErrRecordNotFound,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			repo, dbFilename := setupTestEnvironment(t, tc.initialData)
			ctx := context.Background()

			updatedRecord, err := repo.UpdateRecordInCollection(ctx, tc.resourceName, tc.recordID, tc.recordToUpdate)

			if tc.wantErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}

			// check if data is the expected after operation fails
			if tc.wantReturnedRecord == nil {
				return
			}

			// assert equal record is returned by CreateRecord
			assert.Equal(t, tc.wantReturnedRecord, updatedRecord)

			verifyResourceState(t, ctx, repo, dbFilename, tc.resourceName, tc.wantCollection)
		})
	}
}
