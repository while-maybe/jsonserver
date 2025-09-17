package main

import (
	"context"
	"fmt"
	"jsonserver/internal/adapters/driven/jsonrepo"
	"jsonserver/internal/adapters/driving/httpadapter"
	"jsonserver/internal/assets"
	"jsonserver/internal/config"
	"jsonserver/internal/core/service/resource"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	jsonv2 "encoding/json/v2"
)

func main() {
	fmt.Println(assets.BannerString)
	log.Printf("Starting json server...")

	// detect the operating mode at runtime
	cfg, pipedData := bootstrap()

	var repo resource.Repository
	switch cfg.OpMode {
	case config.ModePipe:
		// create an in-memory repo (only)
		log.Println("INFO: Initialising repository from stdin data.")
		repo = jsonrepo.NewJsonRepositoryFromData(pipedData)

	case config.ModeServer:
		log.Println("INFO: Initialising file-based repository.")

		var err error
		repo, err = jsonrepo.NewJsonRepository(cfg)
		if err != nil {
			log.Fatalf("Failed to create repository: %v", err)
		}

	default:
		log.Fatal("FATAL: Server can't run in unknown mode")
		// shouldn't be here
	}

	// create the context
	appCtx, cancel := context.WithCancel(context.Background())

	// TODO - failsafe cleanup? Unsure
	defer cancel()

	setupSignalHandler(cancel)

	if err := run(appCtx, cfg, repo); err != nil {
		log.Fatalf("FATAL: Application run failed: %v", err)
	}

	log.Println("Server exiting gracefully...")
}

// setupSignalHandler configures a listener for OS signals to trigger a graceful shutdown.
func setupSignalHandler(cancelFunc context.CancelFunc) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // listen to OS interrupt signal

	// clean shutdown sequence
	go func() {
		<-quit
		log.Println("Shutdown signal received...")
		cancelFunc()
	}()
}

// bootstrap determines the operating mode and returns the appropriate config and any piped data.
func bootstrap() (*config.Config, map[string]any) {
	stat, _ := os.Stdin.Stat()

	if (stat.Mode() & os.ModeCharDevice) == 0 {
		log.Println("INFO: Data detected on stdin. Running in pipe mode.")

		cfg := config.Default()
		cfg.OpMode = config.ModePipe

		var pipedData map[string]any
		if err := jsonv2.UnmarshalRead(os.Stdin, &pipedData); err != nil {
			log.Fatalf("FATAL: Failed to parse JSON from stdin: %v", err)
		}

		return cfg, pipedData
	}

	log.Println("INFO: No data on stdin. Running in default server mode.")

	// load the config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Configuration loaded: Server Address=%s, Data Directory=%s", cfg.ServerAddr, cfg.DataDir)

	cfg.OpMode = config.ModeServer

	return cfg, nil
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
