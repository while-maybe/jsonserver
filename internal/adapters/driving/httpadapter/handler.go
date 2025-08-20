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

type Handler struct {
	resourceService resource.Service
}

func NewHandler(svc resource.Service) *Handler {
	return &Handler{
		resourceService: svc,
	}
}

func (h *Handler) SetupRoutes() http.Handler {
	router := chi.NewRouter()

	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)

	router.Get("/{resourceName}", h.GetAllRecords)

	return router
}

func (h *Handler) GetAllRecords(w http.ResponseWriter, r *http.Request) {
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
