package httpadapter

import (
	"errors"
	"jsonserver/internal/core/domain"
	"jsonserver/internal/core/service/resource"
	"log"
	"net/http"
	"time"

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

type ListResponse[T any] struct {
	Count int `json:"count"`
	Data  []T `json:"data"`
}

type SingleObjectResponse[T any] struct {
	Data T `json:"data"`
}

type ErrorResponse struct {
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func NewHandler(svc resource.Service) *Handler {
	return &Handler{
		resourceService: svc,
	}
}

func (h *Handler) decodeJSON(r *http.Request, data any) error {
	if err := jsonv2.UnmarshalRead(r.Body, data); err != nil {
		log.Printf("ERROR: Failed to decode request body: %v", err)
		return err
	}
	return nil
}

func (h *Handler) respondWithJSON(w http.ResponseWriter, responseCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)

	opts := jsonv2.JoinOptions(jsontext.Multiline(true), jsontext.WithIndent("  "))

	if err := jsonv2.MarshalWrite(w, data, opts); err != nil {
		log.Printf("ERROR: Failed to encode response: %v", err)
	}
}

func (h *Handler) handleError(w http.ResponseWriter, err error) {
	var httpStatusCode int
	switch {

	// Not Found Errors
	case errors.Is(err, resource.ErrResourceNotFound), errors.Is(err, resource.ErrRecordNotFound):
		httpStatusCode = http.StatusNotFound

	// Bad Request Errors
	case errors.Is(err, resource.ErrEmptyResourceName), errors.Is(err, resource.ErrEmptyRecordID), errors.Is(err, resource.ErrInvalidRecord), errors.Is(err, resource.ErrNoDataProvided):
		httpStatusCode = http.StatusBadRequest

	// Conflict Errors
	case errors.Is(err, resource.ErrWrongResourceType), errors.Is(err, resource.ErrDuplicateID):
		httpStatusCode = http.StatusConflict

	// Not implemented by the server
	case errors.Is(err, resource.ErrUnknownResourceType):
		log.Printf("FATAL LOGIC ERROR: Handler is not aware of a resource type: %v", err)
		httpStatusCode = http.StatusNotImplemented

	// Default to Server Error
	default:
		log.Printf("ERROR: Unhandled internal error from service: %v", err)
		httpStatusCode = http.StatusInternalServerError
	}

	var response ErrorResponse
	response.Error.Message = err.Error()

	h.respondWithJSON(w, httpStatusCode, response)
}

func (h *Handler) apiV1Router() http.Handler {
	router := chi.NewRouter()
	router.Use(RequestSizeLimit(MaxRequestSize)) // enforce MaxRequestSize limit

	// Read operations - use specific 'recordID' for clarity
	router.With(RequireURLParams("resourceName")).Get("/{resourceName}", h.HandleGetAllRecords)
	router.With(RequireURLParams("resourceName", "recordID")).Get("/{resourceName}/{recordID}", h.HandleGetRecordByID)
	router.Get("/", h.HandleListResources)

	// Write operations with size limits - use generic 'identifier' for flexibility
	router.Group(func(r chi.Router) {
		r.With(RequireURLParams("resourceName")).Post("/{resourceName}", h.HandleCreateRecord)
		r.With(RequireURLParams("resourceName", "identifier")).Put("/{resourceName}/{identifier}", h.HandleUpdate)
		// .Delete() grouped here for future auth
		r.With(RequireURLParams("resourceName", "identifier")).Delete("/{resourceName}/{identifier}", h.HandleDelete)
	})

	return router
}

func (h *Handler) SetupRoutes() http.Handler {
	router := chi.NewRouter()

	router.Use(middleware.RealIP)
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(30 * time.Second))

	router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	router.Mount("/api/v1", h.apiV1Router())

	return router
}

func (h *Handler) HandleGetAllRecords(w http.ResponseWriter, r *http.Request) {
	// cleanly extract the URL parameter
	resourceName := chi.URLParam(r, "resourceName")

	// call the core service
	records, err := h.resourceService.GetAllRecords(r.Context(), resourceName)
	if err != nil {
		h.handleError(w, err)
		return
	}

	if records == nil {
		records = []domain.Record{}
	}

	response := ListResponse[domain.Record]{
		Count: len(records),
		Data:  records,
	}

	h.respondWithJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleGetRecordByID(w http.ResponseWriter, r *http.Request) {
	resourceName := chi.URLParam(r, "resourceName")
	recordID := chi.URLParam(r, "recordID")

	record, err := h.resourceService.GetRecordByID(r.Context(), resourceName, recordID)
	if err != nil {
		h.handleError(w, err)
		return
	}

	response := SingleObjectResponse[domain.Record]{Data: record}

	h.respondWithJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleCreateRecord(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() // needs to happen before as if UnmarshallRead below fails and returns, this never happens
	resourceName := chi.URLParam(r, "resourceName")

	resourceType := h.resourceService.CheckResourceType(r.Context(), resourceName)

	if resourceType != resource.ResourceTypeUnknown && resourceType != resource.ResourceTypeCollection {
		response := ErrorResponse{}
		response.Error.Message = "Method POST not allowed on this resource type. Use PUT for keyed objects."

		h.respondWithJSON(w, http.StatusMethodNotAllowed, response)
		return
	}

	var postData domain.Record
	if err := h.decodeJSON(r, &postData); err != nil {
		h.respondWithJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	record, err := h.resourceService.CreateRecordInCollection(r.Context(), resourceName, postData)
	if err != nil {
		h.handleError(w, err)
		return
	}

	response := SingleObjectResponse[domain.Record]{Data: record}

	h.respondWithJSON(w, http.StatusCreated, response)
}

func (h *Handler) handleUpsertRecordByKey(w http.ResponseWriter, r *http.Request, resourceName, recordKey string) {
	defer r.Body.Close()

	var postData domain.Record

	if err := h.decodeJSON(r, &postData); err != nil {
		h.respondWithJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	record, wasCreated, err := h.resourceService.UpsertRecordInObject(r.Context(), resourceName, recordKey, postData)
	if err != nil {
		h.handleError(w, err)
		return
	}

	var successStatusCode int
	if wasCreated {
		successStatusCode = http.StatusCreated
	} else {
		successStatusCode = http.StatusOK
	}

	response := SingleObjectResponse[domain.Record]{Data: record}

	h.respondWithJSON(w, successStatusCode, response)
}

func (h *Handler) handleDeleteRecordByID(w http.ResponseWriter, r *http.Request, resourceName, recordID string) {
	err := h.resourceService.DeleteRecordFromCollection(r.Context(), resourceName, recordID)
	if err != nil {
		h.handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleDeleteRecordByKey(w http.ResponseWriter, r *http.Request, resourceName, recordKey string) {
	err := h.resourceService.DeleteRecordInObject(r.Context(), resourceName, recordKey)
	if err != nil {
		h.handleError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	resourceName := chi.URLParam(r, "resourceName")
	identifier := chi.URLParam(r, "identifier")

	resourceType := h.resourceService.CheckResourceType(r.Context(), resourceName)

	switch resourceType {

	case resource.ResourceTypeCollection:
		h.handleDeleteRecordByID(w, r, resourceName, identifier)

	case resource.ResourceTypeKeyedObject:
		h.handleDeleteRecordByKey(w, r, resourceName, identifier)

	case resource.ResourceTypeSingular:
		h.handleDeleteRecordByKey(w, r, resourceName, identifier)

	case resource.ResourceTypeUnknown:
		h.handleError(w, resource.ErrResourceNotFound)
	default:
		h.handleError(w, resource.ErrUnknownResourceType)
	}
}

func (h *Handler) handleUpdateRecordByID(w http.ResponseWriter, r *http.Request, resourceName, recordID string) {
	defer r.Body.Close()

	var postData domain.Record

	if err := h.decodeJSON(r, &postData); err != nil {
		h.respondWithJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	record, err := h.resourceService.UpdateRecordInCollection(r.Context(), resourceName, recordID, postData)
	if err != nil {
		h.handleError(w, err)
		return
	}

	response := SingleObjectResponse[domain.Record]{Data: record}

	h.respondWithJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleUpdate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	resourceName := chi.URLParam(r, "resourceName")
	identifier := chi.URLParam(r, "identifier")

	resourceType := h.resourceService.CheckResourceType(r.Context(), resourceName)

	switch resourceType {

	case resource.ResourceTypeCollection:
		h.handleUpdateRecordByID(w, r, resourceName, identifier)

	case resource.ResourceTypeKeyedObject:
		h.handleUpsertRecordByKey(w, r, resourceName, identifier)

	case resource.ResourceTypeSingular:
		h.handleUpsertRecordByKey(w, r, resourceName, identifier)

	case resource.ResourceTypeUnknown:
		h.handleError(w, resource.ErrResourceNotFound)
	default:
		h.handleError(w, resource.ErrUnknownResourceType)
	}
}

func (h *Handler) HandleListResources(w http.ResponseWriter, r *http.Request) {
	resourceNames, err := h.resourceService.ListResources(r.Context())
	if err != nil {
		h.handleError(w, err)
		return
	}

	response := ListResponse[string]{
		Count: len(resourceNames),
		Data:  resourceNames,
	}

	h.respondWithJSON(w, http.StatusOK, response)
}
