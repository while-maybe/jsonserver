package main

import (
	"context"
	"jsonserver/internal/adapters/driven/jsonrepo"
	"jsonserver/internal/adapters/driving/httpadapter"
	"jsonserver/internal/config"
	"jsonserver/internal/core/service/resource"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.Printf("Starting json server...")

	// load the config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Configuration loaded: Server Address=%s, Data Directory=%s", cfg.ServerAddr, cfg.DataDir)

	// create the repo
	repo, err := jsonrepo.NewJsonRepository(cfg.DataDir)
	if err != nil {
		log.Fatalf("Failed to create repository: %v", err)
	}

	log.Printf("Repository initialized successfully.")

	// service and handler
	resourceSvc := resource.NewService(repo)
	apiHandler := httpadapter.NewHandler(resourceSvc)

	// config the server
	server := &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      apiHandler.SetupRoutes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// create the context
	appCtx, cancel := context.WithCancel(context.Background())
	// failsafe cleanup
	defer cancel()

	// start the file watcher
	repo.Watch(appCtx)

	// start the server
	go func() {
		log.Printf("Server starting on %s", cfg.ServerAddr)
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	log.Printf("Server is ready to handle requests at %s", cfg.ServerAddr)

	// clean shutdown sequence
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // listen to OS interrupt signal

	<-quit
	log.Println("Shutdown signal received...")

	cancel()
	log.Println("Signaling background processes to stop...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("FATAL: Server forced to shutdown: %v", err)
	}

	log.Println("Server exiting gracefully...")
}
