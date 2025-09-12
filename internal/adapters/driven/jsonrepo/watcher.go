package jsonrepo

import (
	"context"
	"fmt"
	"log"

	"github.com/fsnotify/fsnotify"
)

type Watcher interface {
	Watch(ctx context.Context)
}

// fsnotifyWatcher is a concrete implementation of the Watcher interface using the fsnotify library.
type fsnotifyWatcher struct {
	watcher    *fsnotify.Watcher
	dataDir    string
	reloadFunc func() error
}

type noOpWatcher struct{}

func (w *noOpWatcher) Watch(ctx context.Context) {}

// NewFsnotifyWatcher creates a new file watcher. It takes a data directory to watch and a reload function to call on events.
func NewFsnotifyWatcher(dataDir string, reloadFunc func() error) (*fsnotifyWatcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	return &fsnotifyWatcher{
		watcher:    fsWatcher,
		dataDir:    dataDir,
		reloadFunc: reloadFunc,
	}, nil
}

// Watch implements the Watcher interface.
func (w *fsnotifyWatcher) Watch(ctx context.Context) {
	go func() {
		defer w.watcher.Close()

		for {
			select {
			case <-ctx.Done():
				log.Println("Stopping file watcher.")
				return

			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}

				// only ops that change data
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Remove) {
					log.Printf("File changed detected: %s. Reloading all data.", event.Name)

					if err := w.reloadFunc(); err != nil {
						log.Printf("Error: failed to hot-reload data: %v", err)
					}
				}

			case err, ok := <-w.watcher.Errors:
				if !ok {
					return
				}
				log.Printf("Error: file watcher error: %v", err)
			}
		}
	}()

	err := w.watcher.Add(w.dataDir)
	if err != nil {
		log.Fatalf("failed to start watching data directory: %v", err)
	}

	log.Printf("Watching for changes in data directory: %s", w.dataDir)
}
