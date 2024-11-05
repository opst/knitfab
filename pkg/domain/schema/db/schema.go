package db

import "context"

// SchemaInterface represents a database schema.
type SchemaInterface interface {
	// Upgrade upgrades the schema to the latest version.
	Upgrade(ctx context.Context) error

	// Version returns the current version of the schema.
	Version(ctx context.Context) (int, error)

	// Context returns a context which is closed when the schema in database is not latest.
	//
	// Args
	//
	// - ctx: The context to be used.
	//
	// Returns
	//
	// - context.Context: The context which will be closed when schema in database is older than reqirement.
	//
	// - context.CancelFunc: The function to cancel the context.
	Context(ctx context.Context) (context.Context, context.CancelFunc)
}
