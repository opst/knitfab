package testhelpers

import (
	"context"
	"strings"
	"time"

	"github.com/opst/knitfab-api-types/misc/rfctime"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
)

// get current timestamp in postgres.
func PGNow(ctx context.Context, conn kpool.Queryer) (time.Time, error) {
	var now time.Time
	err := conn.QueryRow(ctx, `select now()`).Scan(&now)
	if err != nil {
		return time.Time{}, err
	}
	return now, nil
}

// DEPRECATED: to parse text-formatted timestamp, use rfctime for consistency
func ISO8601(str string) (time.Time, error) {
	rt, err := rfctime.ParseRFC3339DateTime(str)
	if err != nil {
		return time.Time{}, err
	}
	return rt.Time(), err
}

func Ref[T interface{}](v T) *T { return &v }

func Deref[T any](v *T) T { return *v }

func PaddingX[S ~string](x int, str S, padStr rune) S {

	runeStr := []rune(str)

	if x <= 0 {
		return ""
	}

	var strnum int
	var padnum int

	if x-len(runeStr) < 0 {
		strnum = x
		padnum = 0
	} else {
		strnum = len(runeStr)
		padnum = x - len(runeStr)
	}

	return S(runeStr[0:strnum]) + S(strings.Repeat(string(padStr), padnum))
}

func Padding16Space[S ~string](str S) S {
	return PaddingX(16, str, ' ')
}

func Padding36[S ~string](str S) S {
	return PaddingX(36, str, '_')
}

func Padding64[S ~string](str S) S {
	return PaddingX(64, str, '_')
}

// like pgpool.Pool.BeginFunc, but always rollback transaction.
//
// # params:
//
// - ctx context.Context
//
// - pool kpg.Pool : connection pool where new transaction begins from.
//
// - f func(kpg.Tx) error : called with a new transaction to be rollbacked.
//
// # returns:
//
// - error : caused by pool.Begin or f.
func BeginFuncToRollback(ctx context.Context, pool kpool.Pool, f func(kpool.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	return f(tx)
}
