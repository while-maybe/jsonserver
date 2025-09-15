package jsonrepo

import (
	"jsonserver/internal/core/domain"
	"math"
)

// dataNormaliser is an unexported struct responsible for transforming data between its raw form (from JSON) and its canonical in-memory form
type dataNormaliser struct{}

func NewDataNormaliser() *dataNormaliser {
	return &dataNormaliser{}
}

func (n *dataNormaliser) normalise(value any) any {
	switch v := value.(type) {
	case float64:
		// if the number has no fractional part, convert and return to int
		if v == math.Trunc(v) {
			return int(v)
		}
		// otherwise keep the float
		return v

	case []any:
		return n.transformSlice(v, n.normalise)

	case map[string]any:
		return n.transformMap(v, n.normalise)

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

func (n *dataNormaliser) denormalise(value any) any {
	switch v := value.(type) {
	case domain.Record:
		return n.transformMap(v, n.denormalise)

	case []any:
		return n.transformSlice(v, n.denormalise)

	default:
		// Keep keyed objects and singular values as-is
		return v
	}
}

// transformSlice takes a collection and applies a transformer function to each item returning a same length collection - think .map() in JS
func (n *dataNormaliser) transformSlice(slice []any, transformer func(any) any) []any {
	result := make([]any, len(slice))

	for i, item := range slice {
		result[i] = transformer(item)
	}

	return result
}

func (n *dataNormaliser) transformMap(m map[string]any, transformer func(any) any) domain.Record {
	normalisedMap := make(map[string]any, len(m))

	for key, value := range m {
		normalisedMap[key] = transformer(value)
	}

	// this conversion is safe as domain.Record IS a map[string]any
	return domain.Record(normalisedMap)
}
