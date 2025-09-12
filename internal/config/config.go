package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

// OpMode is a custom type to represent the application's operating mode - read from configured folder or stdin
type OpMode int

type Config struct {
	ServerAddr string
	DataDir    string
	OpMode     OpMode
}

const (
	ModeServer OpMode = iota
	ModePipe
)

// change here only as it populates both default and env aware configs
var cfgDefaults = map[string]string{
	"SERVER_ADDR": ":8080",
	"DATA_DIR":    "data",
	//"OP_MODE" should not be here as the mode is set at run time
}

// Default return a configuration object with defaults so can bypass .env file or ENV vars
func Default() *Config {
	return &Config{
		ServerAddr: cfgDefaults["SERVER_ADDR"],
		DataDir:    cfgDefaults["DATA_DIR"],
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

	cfg := &Config{
		ServerAddr: getEnv("SERVER_ADDR", cfgDefaults["SERVER_ADDR"]),
		DataDir:    getEnv("DATA_DIR", cfgDefaults["DATA_DIR"]),
	}
	return cfg, nil
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
