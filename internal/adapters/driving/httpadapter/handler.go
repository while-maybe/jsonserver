package httpadapter

import (
	"errors"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
)

const (
	MaxRequestSize = 1024 * 1024 // 1MB max request size
)

type Handler struct {
	resourceService resource.Service
}

func NewHandler(svc resource.Service) *Handler {
	return &Handler{
		resourceService: svc,
	}
}

func RequestSizeLimit(maxSize int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxSize)
			next.ServeHTTP(w, r)
		})
	}
}

func (h *Handler) SetupRoutes() http.Handler {
	router := chi.NewRouter()

	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)

	router.Get("/{resourceName}", h.HandleGetAllRecords)
	router.Get("/{resourceName}/{recordID}", h.HandleGetRecordByID)

	// write operations will have size limits
	router.Group(func(r chi.Router) {
		r.Use(RequestSizeLimit(MaxRequestSize)) // 1MB limit
		r.Post("/{resourceName}", h.HandleCreateRecord)
		r.Put("/{resourceName}/{recordKey}", h.HandleUpsertRecordByKey)
	})

	return router
}

func (h *Handler) HandleGetAllRecords(w http.ResponseWriter, r *http.Request) {
	// cleanly extract the URL parameter
	resourceName := chi.URLParam(r, "resourceName")

	// call the core service
	records, err := h.resourceService.GetAllRecords(r.Context(), resourceName)
	if err != nil {
		if errors.Is(err, resource.ErrEmptyResourceName) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("ERROR: Failed to get all records for '%s': %v", resourceName, err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if records == nil {
		records = []domain.Record{}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))
	if err := jsonv2.MarshalWrite(w, records, opts); err != nil {
		log.Printf("ERROR: Failed to encode response for '%s': %v", resourceName, err)
	}
}

func (h *Handler) HandleGetRecordByID(w http.ResponseWriter, r *http.Request) {
	resourceName := chi.URLParam(r, "resourceName")
	recordID := chi.URLParam(r, "recordID")

	record, err := h.resourceService.GetRecordByID(r.Context(), resourceName, recordID)
	if err != nil {
		if errors.Is(err, resource.ErrEmptyResourceName) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if errors.Is(err, resource.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))
	if err := jsonv2.MarshalWrite(w, record, opts); err != nil {
		log.Printf("ERROR: Failed to encode response for '%s' with ID '%s': %v", resourceName, recordID, err)
	}
}

func (h *Handler) HandleCreateRecord(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() // needs to happen before as if UnmarshallRead below fails and returns, this never happens
	resourceName := chi.URLParam(r, "resourceName")

	resourceType := h.resourceService.CheckResourceType(r.Context(), resourceName)

	if resourceType != resource.ResourceTypeUnknown && resourceType != resource.ResourceTypeCollection {
		http.Error(w, "Method POST not allowed on this resource type. Use PUT for keyed objects.", http.StatusMethodNotAllowed)
		return
	}

	postData := make(map[string]any, 0)

	if err := jsonv2.UnmarshalRead(r.Body, &postData); err != nil {
		log.Printf("ERROR: Failed to decode request for '%s': %v", resourceName, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := h.resourceService.CreateRecordInCollection(r.Context(), resourceName, postData)
	if err != nil {
		var httpStatusCode int
		switch {
		case errors.Is(err, resource.ErrNoDataProvided):
			httpStatusCode = http.StatusBadRequest

		// if checking for duplicates fails (in resource)
		case errors.Is(err, resource.ErrDuplicateID):
			httpStatusCode = http.StatusConflict

		case errors.Is(err, resource.ErrNotFound):
			httpStatusCode = http.StatusNotFound

		default:
			log.Printf("ERROR: Unhandled error from service: %v", err)
			httpStatusCode = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), httpStatusCode)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))
	if err := jsonv2.MarshalWrite(w, record, opts); err != nil {
		log.Printf("ERROR: Failed to encode response for '%s': %v", resourceName, err)
	}
}

func (h *Handler) HandleUpsertRecordByKey(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	resourceName := chi.URLParam(r, "resourceName")
	recordKey := chi.URLParam(r, "recordKey")

	if resourceName == "" || recordKey == "" {
		http.Error(w, "Resource name and record key are required", http.StatusBadRequest)
		return
	}

	resourceType := h.resourceService.CheckResourceType(r.Context(), resourceName)

	if resourceType != resource.ResourceTypeUnknown && resourceType != resource.ResourceTypeKeyedObject {
		http.Error(w, "Method PUT not allowed on this resource type. Use POST for collection objects.", http.StatusMethodNotAllowed)
		return
	}

	postData := make(map[string]any)

	if err := jsonv2.UnmarshalRead(r.Body, &postData); err != nil {
		log.Printf("ERROR: Failed to decode request for '%s': %v", resourceName, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, wasCreated, err := h.resourceService.UpsertRecordInObject(r.Context(), resourceName, recordKey, postData)
	if err != nil {
		var httpStatusCode int
		switch {
		case errors.Is(err, resource.ErrNoDataProvided):
			httpStatusCode = http.StatusBadRequest

		// if checking for duplicates fails (in resource)
		case errors.Is(err, resource.ErrEmptyRecordKey):
			httpStatusCode = http.StatusBadRequest

		case errors.Is(err, resource.ErrInvalidRecord):
			httpStatusCode = http.StatusBadRequest

		case errors.Is(err, resource.ErrInvalidResourceName):
			httpStatusCode = http.StatusBadRequest

		default:
			log.Printf("ERROR: Unhandled error from service: %v", err)
			httpStatusCode = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), httpStatusCode)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	var successStatus int
	if wasCreated {
		successStatus = http.StatusCreated
	} else {
		successStatus = http.StatusOK
	}
	w.WriteHeader(successStatus)

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))
	if err := jsonv2.MarshalWrite(w, record, opts); err != nil {
		log.Printf("ERROR: Failed to encode response for '%s': %v", resourceName, err)
	}
}
