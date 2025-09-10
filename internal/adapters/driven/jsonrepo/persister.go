package jsonrepo

import (
	"encoding/json/jsontext"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	jsonv2 "encoding/json/v2"
)

const defaultFilePermissions = os.FileMode(0644)

// Needs to be exported so we can call in when in tests
type Persister interface {
	Persist(data map[string]any) error
}

type FilePersister struct {
	dataDir string
}

func NewFilePersister(dataDir string) *FilePersister {
	return &FilePersister{dataDir: dataDir}
}

func (fp *FilePersister) Persist(data map[string]any) error {
	keysInCache := make(map[string]bool)

	// figure the keys that should actually exist as individual json data files
	for resourceName, resourceData := range data {
		keysInCache[resourceName] = true

		// generate filenames based on top-resource names
		filename := fmt.Sprintf("%s.json", resourceName)
		filePath := filepath.Join(fp.dataDir, filename)

		file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFilePermissions)

		if err != nil {
			return fmt.Errorf("error opening file %s for persistence: %w", filePath, err)
		}
		defer file.Close()

		opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))
		if err := jsonv2.MarshalWrite(file, resourceData, opts); err != nil {
			return fmt.Errorf("error writing JSON to file %s: %w", filePath, err)
		}
	}

	// cleanup phase
	entries, err := os.ReadDir(fp.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory for cleanup: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		resourceName := strings.TrimSuffix(entry.Name(), ".json")

		// now if a file exists without a matching resourceName in-memory we delete
		if _, exists := keysInCache[resourceName]; !exists {
			filePath := filepath.Join(fp.dataDir, entry.Name())

			if err := os.Remove(filePath); err != nil {
				log.Printf("WARN: failed to remove orphaned resource file '%s': %v", filePath, err)
			}
		}
	}

	return nil
}
