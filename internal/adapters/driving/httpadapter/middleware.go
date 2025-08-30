package httpadapter

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func RequestSizeLimit(maxSize int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			r.Body = http.MaxBytesReader(w, r.Body, maxSize)
			next.ServeHTTP(w, r)
		})
	}
}

// this is the factory
func RequireURLParams(params ...string) func(http.Handler) http.Handler {
	// this is the middleware function that gets returned
	return func(next http.Handler) http.Handler {

		// this is the actual handler that will be used for each request
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			for _, param := range params {

				if chi.URLParam(r, param) == "" {

					msg := fmt.Sprintf("Bad request: URL parameter '%s' is required", param)
					http.Error(w, msg, http.StatusBadRequest)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
