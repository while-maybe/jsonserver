package domain

import (
	"errors"
	"fmt"
)

type Record map[string]any

func (r Record) ID() (string, bool) {
	id, ok := r["id"]
	if !ok {
		return "", false
	}

	return fmt.Sprintf("%v", id), true
}

func NewFromMap(data map[string]any) (Record, error) {
	if data == nil {
		return nil, errors.New("cannot create record from nil data")
	}

	return Record(data), nil
}
