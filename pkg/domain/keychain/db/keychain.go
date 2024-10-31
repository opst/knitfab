package db

import "context"

// Keychain is an interface for synchronizing access to keychain entries.
type KeychainInterface interface {
	// Lock locks a keychain entry by name and executes the critical section.
	//
	// # Args
	//
	// - ctx (context.Context): The context of the operation.
	//
	// - name (KeyChainName): The name of the keychain.
	//
	// - criticalSection (func() error): The critical section to execute.
	// If and only if in the critical section, you can update the keychain.
	// If the critical section returns an error, the transaction will be rolled back.
	//
	// # Returns
	//
	// - error: An error if the operation failed.
	//
	Lock(ctx context.Context, name string, criticalSection func(ctx context.Context) error) error
}
