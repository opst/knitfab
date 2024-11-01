package db

import (
	"context"

	"github.com/opst/knitfab/pkg/domain"
)

type Interface interface {
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
	Pop(context.Context, func(domain.Garbage) error) (bool, error)
}
