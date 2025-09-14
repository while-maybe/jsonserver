package jsonrepo

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fsnotify/fsnotify"
)

type Watcher interface {
	Watch(ctx context.Context)
}

// fsnotifyWatcher is a concrete implementation of the Watcher interface using the fsnotify library.
type fsnotifyWatcher struct {
	watcher          *fsnotify.Watcher
	dataDir          string
	reloadFunc       func() error
	isInternalChange func() bool

	// timer for debouncing events
	debounceTimer *time.Timer
	debounceDur   time.Duration
}

type noOpWatcher struct{}

func NewNoOpWatcher() *noOpWatcher {
	return &noOpWatcher{}
}

func (w *noOpWatcher) Watch(ctx context.Context) {}

// NewFsnotifyWatcher creates a new file watcher. It takes a data directory to watch and a reload function to call on events.
func NewFsnotifyWatcher(dataDir string, reloadFunc func() error, isInternalChange func() bool) (*fsnotifyWatcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	return &fsnotifyWatcher{
		watcher:          fsWatcher,
		dataDir:          dataDir,
		reloadFunc:       reloadFunc,
		isInternalChange: isInternalChange,
		debounceDur:      100 * time.Millisecond,
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

				if w.isInternalChange() { // if the modification comes from in-memory cache don't reload
					continue
				}

				// don't do anything if the event won't modify files
				isModifyEvent := event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Remove)
				if !isModifyEvent {
					continue
				}

				// log.Printf("File system event detected: %s. Arming reload timer.", event.Op)

				// there is a timer already going
				if w.debounceTimer != nil {
					w.debounceTimer.Reset(w.debounceDur)
					continue
				}

				// event doesn't get captured, it's only a reference to the for loop variable that gets used on each iteration - we need to copy
				eventCopy := event
				reloadFunc := func() {
					log.Printf("File changed detected: %s. Reloading all data.", eventCopy.Name)

					if err := w.reloadFunc(); err != nil {
						log.Printf("Error: failed to hot-reload data: %v", err)
					}

					w.debounceTimer = nil
				}

				w.debounceTimer = time.AfterFunc(w.debounceDur, reloadFunc)

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
