package main

import (
	"context"
	"fmt"
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

	// create the context
	appCtx, cancel := context.WithCancel(context.Background())

	// TODO - failsafe cleanup? Unsure
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // listen to OS interrupt signal

	// clean shutdown sequence
	go func() {
		<-quit
		log.Println("Shutdown signal received...")
		cancel()
	}()

	if err := run(appCtx, cfg, repo); err != nil {
		log.Fatalf("FATAL: Application run failed: %v", err)
	}

	log.Println("Server exiting gracefully...")
}

func run(appCtx context.Context, cfg *config.Config, repo resource.Repository) error {
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

	jr, ok := repo.(*jsonrepo.JsonRepository)
	if ok {
		log.Printf("INFO: File-based repository detected, starting watcher.")
		jr.Watch(appCtx)
	}

	// start the server
	go func() {
		log.Printf("Server starting on %s", cfg.ServerAddr)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ERROR: Server listen error: %v", err)
		}
	}()

	// listen for context cancellation
	<-appCtx.Done()
	log.Println("Context cancelled, initiating server shutdown.")

	// graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("FATAL: Server forced to shutdown: %v", err)
	}

	return nil
}
