package os

import "os"

// Get environment variable. If missing/empty, return fallback value.
func GetEnvOr(name, fallback string) string {
	val := os.Getenv(name)
	if val == "" {
		return fallback
	}
	return val
}
