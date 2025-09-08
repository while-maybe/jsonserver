package copier

import (
	"fmt"
	"jsonserver/internal/core/domain" // It needs to know about domain.Record
)

func DeepCopy[T any](src T) (T, error) {
	var zero T

	copied := deepCopyValue(any(src))
	if result, ok := copied.(T); ok {
		return result, nil
	}

	return zero, fmt.Errorf("deep copy failed: expected %T, got %T", zero, copied)
}

// deepCopyValue handles the actual deep copying logic for your specific types
func deepCopyValue(src any) any {
	if src == nil {
		return nil
	}

	switch v := src.(type) {
	case domain.Record:
		if v == nil {
			return domain.Record(nil)
		}
		dst := make(domain.Record, len(v))
		for key, val := range v {
			dst[key] = deepCopyValue(val)
		}
		return dst

	case map[string]domain.Record:
		if v == nil {
			return domain.Record(nil)
		}
		dst := make(map[string]domain.Record, len(v))
		for key, val := range v {
			dst[key] = deepCopyValue(val).(domain.Record)
		}
		return dst

	case map[string]any:
		if v == nil {
			return map[string]any(nil)
		}
		dst := make(map[string]any, len(v))
		for key, val := range v {
			dst[key] = deepCopyValue(val)
		}
		return dst

	case []domain.Record:
		if v == nil {
			return []domain.Record(nil)
		}
		dst := make([]domain.Record, len(v))
		for i, record := range v {
			dst[i] = deepCopyValue(record).(domain.Record)
		}
		return dst

	case []any:
		if v == nil {
			return []any(nil)
		}
		dst := make([]any, len(v))
		for i, val := range v {
			dst[i] = deepCopyValue(val)
		}
		return dst

	// Handle primitive types that don't need copying
	case string, int, int64, float64, bool, int32, float32:
		return v

	default:
		// For any other types, return as-is (assumes they're immutable primitives)
		return v
	}
}
