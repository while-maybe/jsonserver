package jsonrepo

import (
	"encoding/json/jsontext"
	"fmt"
	"os"

	jsonv2 "encoding/json/v2"
)

const defaultFilePermissions = os.FileMode(0644)

// Needs to be exported so we can call in when in tests
type Persister interface {
	Persist(data map[string]any) error
}

type FilePersister struct {
	filename string
}

func NewFilePersister(filename string) *FilePersister {
	return &FilePersister{filename: filename}
}

func (fp *FilePersister) Persist(data map[string]any) error {
	f, err := os.OpenFile(fp.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, defaultFilePermissions)

	if err != nil {
		return fmt.Errorf("opening file %s for persistence: %w", fp.filename, err)
	}
	defer f.Close()

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))

	if err := jsonv2.MarshalWrite(f, data, opts); err != nil {
		return fmt.Errorf("writing JSON to file %s: %w", fp.filename, err)
	}
	return nil
}
