package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// OpMode is a custom type to represent the application's operating mode - read from configured folder or stdin
type OpMode int

type Config struct {
	ServerAddr string
	DataDir    string
	OpMode     OpMode
	// persistence strategy settings
	PersistenceMode  PersistenceMode
	PersistenceTimer time.Duration
	// delay settings
	EnableDelay bool
	BaseDelay   time.Duration
	RandDelay   time.Duration
}

// PersistenceMode is a custom type for defining the persistence strategy. It is defined here to be the single source of truth for configuration.
type PersistenceMode int

const (
	// ModeImmediateSync writes every change to disk instantly and blocks the API call.
	ModeImmediateSync PersistenceMode = iota
	// ModeImmediateAsync writes every change to disk instantly but does NOT block the API call.
	ModeImmediateAsync
	// ModeBatched groups writes and saves them on a timer.
	ModeBatched
)

const (
	ModeServer OpMode = iota
	ModePipe
)

// change here only as it populates both default and env aware configs
var cfgDefaults = map[string]string{
	"SERVER_ADDR": ":8080",
	"DATA_DIR":    "data",
	// "OP_MODE" should not be here as the mode is set at run time
	// persistence settings
	"PERSISTENCE_MODE":  "immediate_async",
	"PERSISTENCE_TIMER": "5000ms",
	// delay settings
	"ENABLE_DELAY": "false",
	"BASE_DELAY":   "0s",
	"RAND_DELAY":   "0s",
}

// Default return a configuration object with defaults so can bypass .env file or ENV vars
func Default() *Config {
	// Parse the default string to get the type-safe enum.
	// safe to ignore the error as this defined by us just above
	defaultPersistenceMode, _ := parsePersistenceMode(cfgDefaults["PERSISTENCE_MODE"])
	defaultPersistenceTimer, _ := time.ParseDuration(cfgDefaults["PERSISTENCE_TIMER"])

	// delay settings
	defaultEnableDelay, _ := strconv.ParseBool(cfgDefaults["ENABLE_DELAY"])
	defaultBaseDelay, _ := time.ParseDuration(cfgDefaults["BASE_DELAY"])
	defaultRandDelay, _ := time.ParseDuration(cfgDefaults["RAND_DELAY"])

	return &Config{
		ServerAddr: cfgDefaults["SERVER_ADDR"],
		DataDir:    cfgDefaults["DATA_DIR"],
		// persistence settings
		PersistenceMode:  defaultPersistenceMode,
		PersistenceTimer: defaultPersistenceTimer,
		// delay settings
		EnableDelay: defaultEnableDelay,
		BaseDelay:   defaultBaseDelay,
		RandDelay:   defaultRandDelay,
	}
}

// Load creates a config by loading values from env vars falling back to defaults if these don't exist
func Load() (*Config, error) {
	// Try to load a standard ".env" file. It's not an error if it doesn't exist.
	if err := loadEnvFile(".env"); err != nil {

		if !os.IsNotExist(errors.Unwrap(err)) {
			// If it's some other relevant error return it.
			return nil, err
		}
	}

	modeStr := getEnv("PERSISTENCE_MODE", cfgDefaults["PERSISTENCE_MODE"])
	persistenceMode, err := parsePersistenceMode(modeStr)
	if err != nil {
		return nil, err
	}

	timerStr := getEnv("PERSISTENCE_TIMER", cfgDefaults["PERSISTENCE_TIMER"])
	persistenceTimer, err := time.ParseDuration(timerStr)
	if err != nil {
		return nil, fmt.Errorf(`config error: a time duration value is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h": %v`, err)
	}

	enableDelayStr := getEnv("ENABLE_DELAY", cfgDefaults["ENABLE_DELAY"])
	enableDelay, err := strconv.ParseBool(enableDelayStr)
	if err != nil {
		return nil, fmt.Errorf(`ENABLE_DELAY should be "true" of "false"`)
	}

	baseDelayStr := getEnv("BASE_DELAY", cfgDefaults["BASE_DELAY"])
	baseDelay, err := time.ParseDuration(baseDelayStr)
	if err != nil {
		return nil, fmt.Errorf(`config error: a time duration value is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h": %v`, err)
	}

	randDelayStr := getEnv("RAND_DELAY", cfgDefaults["RAND_DELAY"])
	randDelay, err := time.ParseDuration(randDelayStr)
	if err != nil {
		return nil, fmt.Errorf(`config error: a time duration value is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h": %v`, err)
	}

	cfg := &Config{
		ServerAddr:       getEnv("SERVER_ADDR", cfgDefaults["SERVER_ADDR"]),
		DataDir:          getEnv("DATA_DIR", cfgDefaults["DATA_DIR"]),
		PersistenceMode:  persistenceMode,
		PersistenceTimer: persistenceTimer,
		EnableDelay:      enableDelay,
		BaseDelay:        baseDelay,
		RandDelay:        randDelay,
	}
	return cfg, nil
}

func parsePersistenceMode(modeStr string) (PersistenceMode, error) {
	var persistenceMode PersistenceMode
	switch strings.ToLower(modeStr) {
	case "immediate_sync":
		persistenceMode = ModeImmediateSync
	case "immediate_async":
		persistenceMode = ModeImmediateAsync
	case "batched":
		persistenceMode = ModeBatched
	default:
		return 0, fmt.Errorf("invalid PERSISTENCE_MODE: '%s'. valid options are 'immediate_sync', 'immediate_async', 'batched'", modeStr)
	}

	return persistenceMode, nil
}

// getEnv returns the value of an environment var or the default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// loadEnvFile attempts to read config data pairs from the filename parameter to set as env vars. Returns an error if loading fails
func loadEnvFile(filename string) error {
	file, err := os.Open(filename)

	if err != nil {
		return fmt.Errorf("could not open env file %s: %w", filename, err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for lineNum := 1; scanner.Scan(); lineNum++ {
		line := strings.TrimSpace(scanner.Text())

		// ignore blank lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2) // splits in parts on the first '=' it finds
		if len(parts) != 2 {
			return fmt.Errorf("invalid line %d in %s: %s", lineNum, filename, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if len(value) >= 2 {
			// remove quotes if they exist - quick and dirty for now
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}

		// now set from file if it the env var does not already exist
		if os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading %s: %w", filename, err)
	}

	return nil
}

// below was the original simpler idea but loading from ENV VARS only
//
//

// type ConfigMap map[string]string

// // NewDefaultsConfigMap creates a new configuration map with defaults
// func NewDefaultsConfigMap() ConfigMap {
// 	return ConfigMap{
// 		"SERVER_ADDR": ":8080",
// 		"DATA_DIR":    "data",
// 	}
// }

// // LoadFromConfigMap creates a Config using the ConfigMap approach
// func LoadFromConfigMap() *Config {
// 	cm := NewDefaultsConfigMap()

// 	return &Config{
// 		ServerAddr: cm.Get("SERVER_ADDR", ":8080"),
// 		DataDir:    cm.Get("DATA_DIR", "data"),
// 	}
// }

// // Get returns the value for key, falling back to defaultValue if not found
// func (c ConfigMap) Get(key, defaultValue string) string {
// 	// Check environment first
// 	if envValue := os.Getenv(key); envValue != "" {
// 		return envValue
// 	}

// 	if val, exists := c[key]; exists {
// 		return val
// 	}

// 	return defaultValue
// }
