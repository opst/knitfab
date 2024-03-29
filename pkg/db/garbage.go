package db

import "context"

type Garbage struct {
	KnitId    string
	VolumeRef string
}

type GarbageInterface interface {
	// pop garbage item.
	//
	// Args
	//
	// - context.Context
	//
	// - func(Garbage) error: handler with poped item.
	//   If this handler returns error, popped item will be rolled back.
	//   Otherwise, popped garbage will be removed from DB.
	//
	// Return
	//
	// - bool: if an item is popped
	//
	// - error
	Pop(context.Context, func(Garbage) error) (bool, error)
}
