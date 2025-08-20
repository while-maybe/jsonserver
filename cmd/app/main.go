package main

import (
	"jsonserver/internal/adapters/driven/jsonrepo"
	"jsonserver/internal/adapters/driving/httpadapter"
	"jsonserver/internal/core/service/resource"
	"log"
	"net/http"
)

func main() {
	log.Printf("Starting json server...")

	dbFilename := "db.json"
	listenAddr := ":8080"

	repo, err := jsonrepo.NewJsonRepository(dbFilename)
	if err != nil {
		log.Fatalf("Failed to initialize repository: %v", err)
	}

	log.Printf("Repository for '%s' initialized successfully.", dbFilename)

	resourceSvc := resource.NewService(repo)

	httpHandler := httpadapter.NewHandler(resourceSvc)

	router := httpHandler.SetupRoutes()

	log.Printf("Server is ready to handle requests at %s", listenAddr)

	if err := http.ListenAndServe(listenAddr, router); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
