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

func (r Record) SetID(id string) {
	r["id"] = id
}

func NewFromMap(data map[string]any) (Record, error) {
	if data == nil {
		return nil, errors.New("cannot create record from nil data")
	}

	return Record(data), nil
}

func (r Record) Validate() error {
	if r == nil {
		return errors.New("record cannot be nil")
	}

	if idValue, ok := r["id"]; ok {
		// type assertion here is better as opposed to simply use .ID()
		idString, ok := idValue.(string)
		if !ok {
			return errors.New("record ID must be a string")
		}
		if idString == "" {
			return errors.New("record ID cannot be an empty string")
		}
	}
	return nil
}
