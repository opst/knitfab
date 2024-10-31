package proxy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	kpool "github.com/opst/knitfab/pkg/conn/db/postgres/pool"
	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy"
	intr "github.com/opst/knitfab/pkg/conn/db/postgres/pool/proxy/internal"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/cmp"
)

// capture value to channel.
//
// # params
// - ret R: the value which func(T)R will return
//
// # return
// - <-chan : channel which emits a value captured
// - func(T)R : captureing function sends value to chan.
func capture[T any, R any](ret R) (<-chan T, func(T) R) {
	ch := make(chan T, 1)
	capt := func(t T) R {
		ch <- t
		return ret
	}
	return ch, capt
}

func TestCapture(t *testing.T) {
	ch, cap := capture[string](42)
	ret := cap("hello world")

	if ret != 42 {
		t.Error("unexpected return from capturing func")
	}

	if msg, ok := <-ch; ok && msg != "hello world" {
		t.Error("captured value is wrong")
	}
}

type eventType string

const (
	beforeQuery    eventType = "before query"
	afterQuery     eventType = "after query"
	beforeCommit   eventType = "before commit"
	afterCommit    eventType = "after commit"
	beforeRollback eventType = "before callback"
	afterRollback  eventType = "after callback"
	beforeExitTx   eventType = "before exit tx"
	afterExitTx    eventType = "after exit tx"
)

type tracker struct {
	timeline []eventType
}

func (t *tracker) beforeQuery() {
	t.timeline = append(t.timeline, beforeQuery)
}
func (t *tracker) beforeCommit() {
	t.timeline = append(t.timeline, beforeCommit)
}
func (t *tracker) beforeRollback() {
	t.timeline = append(t.timeline, beforeRollback)
}
func (t *tracker) beforeExitTx() {
	t.timeline = append(t.timeline, beforeExitTx)
}
func (t *tracker) afterQuery() {
	t.timeline = append(t.timeline, afterQuery)
}
func (t *tracker) afterCommit() {
	t.timeline = append(t.timeline, afterCommit)
}
func (t *tracker) afterRollback() {
	t.timeline = append(t.timeline, afterRollback)
}
func (t *tracker) afterExitTx() {
	t.timeline = append(t.timeline, afterExitTx)
}

func eventTrack() (*tracker, *proxy.SQLEvents) {
	t := &tracker{}
	events := proxy.NewPgxEvents()
	events.Query.
		Before(t.beforeQuery).
		After(t.afterQuery)

	events.Commit.
		Before(t.beforeCommit).
		After(t.afterCommit)

	events.Rollback.
		Before(t.beforeRollback).
		After(t.afterRollback)

	events.ExitTx.
		Before(t.beforeExitTx).
		After(t.afterExitTx)
	return t, events
}

type FakeRows struct{}

var _ pgx.Rows = &FakeRows{}

func (fr *FakeRows) Close()                        {}
func (fr *FakeRows) Err() error                    { return nil }
func (fr *FakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (fr *FakeRows) FieldDescriptions() []pgproto3.FieldDescription {
	return []pgproto3.FieldDescription{}
}
func (fr *FakeRows) Next() bool                     { return false }
func (fr *FakeRows) Scan(dest ...interface{}) error { return errors.New("empty") }
func (fr *FakeRows) Values() ([]interface{}, error) { return nil, errors.New("empty") }
func (fr *FakeRows) RawValues() [][]byte            { return [][]byte{} }

// ---
// PoolProxy
// ---

func TestPoolProxy_Acquire(t *testing.T) {

	t.Run("it proxies method call when connections is acquried successfully", func(t *testing.T) {
		ctx := context.Background()

		connAcquired := &intr.FakeConn{}

		innerPool := &intr.FakePool{}
		innerPool.NextAcquire.Conn = connAcquired

		testee := proxy.Wrap(innerPool)

		actual, err := testee.Acquire(ctx)

		if actual == nil {
			t.Fatal("connection is not proxied")
		}
		if err != nil {
			t.Fatal("unexpected error is returned")
		}

		cp, ok := actual.(*proxy.ConnProxy)
		if !ok {
			t.Fatal("acquired conn is not ConnProxy")
		}

		if cp.Base != connAcquired {
			t.Error("it does not wrap acquired connection")
		}

		if cp.Events() != testee.Events() {
			t.Error("it does not pass events to an acquired connection")
		}
	})

	t.Run("it proxies method call when connections is not acquired", func(t *testing.T) {
		ctx := context.Background()
		errOnAcquire := errors.New("error")

		innerPool := &intr.FakePool{}
		innerPool.NextAcquire.Err = errOnAcquire

		testee := proxy.Wrap(innerPool)

		actual, err := testee.Acquire(ctx)

		if actual != nil {
			t.Fatal("unexpected connection is returned")
		}
		if err != errOnAcquire {
			t.Fatal("unexpected error is returned")
		}
	})
}

func TestPoolProxy_AcquireAllIdle(t *testing.T) {
	t.Run("it proxies method call", func(t *testing.T) {
		ctx := context.Background()
		idleConnections := []*intr.FakeConn{{}, {}, {}}

		innerPool := &intr.FakePool{}
		innerPool.NextAcquireAllIdle = utils.Map(
			idleConnections,
			func(c *intr.FakeConn) kpool.Conn { return c },
		)

		testee := proxy.Wrap(innerPool)

		actual := testee.AcquireAllIdle(ctx)

		if cmp.SliceEqWith(
			actual, idleConnections,
			func(a kpool.Conn, x *intr.FakeConn) bool {
				aCp, ok := a.(*proxy.ConnProxy)
				return ok &&
					aCp.Base != x &&
					aCp.Events() != testee.Events()
			},
		) {
			t.Error("it does not wrap connections")
		}
	})
}

func TestPoolProxy_Begin(t *testing.T) {
	t.Run("it proxies method call when transaction is started succesfully", func(t *testing.T) {
		ctx := context.Background()
		tx := &intr.FakeTx{}

		innerPool := &intr.FakePool{}
		innerPool.NextBegin.Tx = tx

		testee := proxy.Wrap(innerPool)

		actual, err := testee.Begin(ctx)

		if err != nil {
			t.Fatal("unexpected error")
		}

		txp, ok := actual.(*proxy.Tx)
		if !ok {
			t.Fatal("transaction type is unexpected one")
		}

		if txp.Base != tx {
			t.Error("it does not wrap transaction")
		}

		if txp.Events() != testee.Events() {
			t.Error("it does not pass events to transaction")
		}
	})

	t.Run("it proxies method call when transaction is not started", func(t *testing.T) {
		ctx := context.Background()
		errInBegin := errors.New("error")

		innerPool := &intr.FakePool{}
		innerPool.NextBegin.Err = errInBegin

		testee := proxy.Wrap(innerPool)

		actual, err := testee.Begin(ctx)

		if err != errInBegin {
			t.Error("unexpected error")
		}

		if actual != nil {
			t.Error("unexpected transaction")
		}
	})
}

func TestPoolProxy_BeginTx(t *testing.T) {
	t.Run("it proxies method call when transaction is started succesfully", func(t *testing.T) {
		ctx := context.Background()
		tx := &intr.FakeTx{}

		innerPool := &intr.FakePool{}
		innerPool.NextBeginTx.Tx = tx

		testee := proxy.Wrap(innerPool)

		actual, err := testee.BeginTx(ctx, pgx.TxOptions{})

		if err != nil {
			t.Fatal("unexpected error")
		}

		txp, ok := actual.(*proxy.Tx)
		if !ok {
			t.Fatal("transaction type is unexpected one")
		}

		if txp.Base != tx {
			t.Error("it does not wrap transaction")
		}

		if txp.Events() != testee.Events() {
			t.Error("it does not pass events to transaction")
		}
	})

	t.Run("it proxies method call when transaction is not started", func(t *testing.T) {
		ctx := context.Background()
		errInBegin := errors.New("error")

		innerPool := &intr.FakePool{}
		innerPool.NextBeginTx.Err = errInBegin

		testee := proxy.Wrap(innerPool)

		actual, err := testee.BeginTx(ctx, pgx.TxOptions{})

		if err != errInBegin {
			t.Error("unexpected error")
		}

		if actual != nil {
			t.Error("unexpected transaction")
		}
	})
}

func TestPoolProxy_Config(t *testing.T) {
	t.Run("it proxies to the inner pool", func(t *testing.T) {
		conf := &pgxpool.Config{}

		inner := &intr.FakePool{}
		inner.NextConfig = conf

		testee := proxy.Wrap(inner)
		if testee.Config() != conf {
			t.Error("it does not proxy to the inner object.")
		}
	})
}

func TestPoolProxy_Ping(t *testing.T) {
	t.Run("it proxies to the inner pool", func(t *testing.T) {
		pingErr := errors.New("no pongs")

		inner := &intr.FakePool{}
		inner.NextPing = pingErr

		testee := proxy.Wrap(inner)
		if testee.Ping(context.Background()) != pingErr {
			t.Error("it does not proxy to the inner object.")
		}
	})
}

// ---
// TxProxy
// ---

func TestTxProxy_Begin(t *testing.T) {
	t.Run("it proxies method call when transaction is started successfully", func(t *testing.T) {
		ctx := context.Background()
		innerTx := &intr.FakeTx{}

		subTx := &intr.FakeTx{}
		innerTx.NextBegin.Tx = subTx

		testee := proxy.WrapTx(innerTx, proxy.NewPgxEvents())

		actual, err := testee.Begin(ctx)

		if err != nil {
			t.Fatal("unexpected error")
		}

		txp, ok := actual.(*proxy.Tx)
		if !ok {
			t.Fatal("transaction type is unexpected one")
		}

		if txp.Base != subTx {
			t.Error("it does not wrap transaction")
		}

		if txp.Events() != testee.Events() {
			t.Error("it does not pass events to transaction")
		}
	})

	t.Run("it proxies method call when transaction is not started", func(t *testing.T) {
		ctx := context.Background()
		errInBegin := errors.New("error")

		innerTx := &intr.FakeTx{}
		innerTx.NextBegin.Err = errInBegin

		testee := proxy.WrapTx(innerTx, proxy.NewPgxEvents())

		actual, err := testee.Begin(ctx)

		if err != errInBegin {
			t.Fatal("unexpected error")
		}
		if actual != nil {
			t.Error("it creates transaction, unexpectedly")
		}
	})
}

func TestTxProxy_Commit(t *testing.T) {
	t.Run("it proxies method call when commit is done successfully", func(t *testing.T) {
		ctx := context.Background()
		errInCommit := errors.New("err")

		innerTx := &intr.FakeTx{}
		innerTx.NextCommit = errInCommit

		tracker, events := eventTrack()
		testee := proxy.WrapTx(innerTx, events)

		err := testee.Commit(ctx)
		if err != errInCommit {
			t.Error("unexpected error is returned")
		}

		if cmp.SliceEq(tracker.timeline, []eventType{
			beforeExitTx, beforeCommit, afterCommit, afterExitTx,
		}) {
			t.Error("event sequence is wrong")
		}
	})
}

func TestTxProxy_Rollback(t *testing.T) {
	t.Run("it proxies method call when commit is done successfully", func(t *testing.T) {
		ctx := context.Background()
		errInRollback := errors.New("err")

		innerTx := &intr.FakeTx{}
		innerTx.NextRollback = errInRollback

		tracker, events := eventTrack()

		testee := proxy.WrapTx(innerTx, events)

		err := testee.Rollback(ctx)
		if err != errInRollback {
			t.Error("unexpected error is returned")
		}

		if cmp.SliceEq(tracker.timeline, []eventType{
			beforeExitTx, beforeRollback, afterRollback, afterExitTx,
		}) {
			t.Error("event sequence is wrong")
		}
	})
}

func TestTxProxy_Exec(t *testing.T) {
	t.Run("it proxies method call when exec is done successfully", func(t *testing.T) {

		ctx := context.Background()
		commandTagInExec := pgconn.CommandTag([]byte("fake command tag"))

		innerTx := &intr.FakeTx{}
		innerTx.NextExec.CommandTag = commandTagInExec

		tracker, events := eventTrack()

		testee := proxy.WrapTx(innerTx, events)

		commandTag, err := testee.Exec(ctx, `update "table" ("column") values ($1);`, 42)

		if err != nil {
			t.Error("unexpected error is returned")
		}

		if !cmp.SliceEq(commandTag, commandTagInExec) {
			t.Error("it does not proxy method: wrong command tag is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})

	t.Run("it proxies method call when exec has failed", func(t *testing.T) {

		ctx := context.Background()
		errInExec := errors.New("error")

		innerTx := &intr.FakeTx{}
		innerTx.NextExec.Err = errInExec

		tracker, events := eventTrack()

		testee := proxy.WrapTx(innerTx, events)

		commandTag, err := testee.Exec(ctx, `update "table" ("column") values ($1);`, 42)

		if err != errInExec {
			t.Error("unexpected error is returned")
		}

		if !cmp.SliceEq(commandTag, pgconn.CommandTag([]byte{})) {
			t.Error("it does not proxy method: non-empty command tag is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})
}

func TestTxProxy_Query(t *testing.T) {
	t.Run("it proxies method call when exec is done successfully", func(t *testing.T) {

		ctx := context.Background()
		expectedRows := &FakeRows{}

		innerTx := &intr.FakeTx{}
		innerTx.NextQuery.Rows = expectedRows

		tracker, events := eventTrack()

		testee := proxy.WrapTx(innerTx, events)

		rows, err := testee.Query(ctx, `select * from  "table" where "column" = $1;`, 42)

		if err != nil {
			t.Error("unexpected error is returned")
		}

		if expectedRows != rows {
			t.Error("it does not proxy method: wrong Rows are returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})

	t.Run("it proxies method call when exec has failed", func(t *testing.T) {

		ctx := context.Background()
		expectedErr := errors.New("error")

		innerTx := &intr.FakeTx{}
		innerTx.NextQuery.Err = expectedErr

		tracker, events := eventTrack()

		testee := proxy.WrapTx(innerTx, events)

		row, err := testee.Query(ctx, `select * from "table" where "column" = $1;`, 42)

		if err != expectedErr {
			t.Error("unexpected error is returned")
		}

		if row != nil {
			t.Error("it does not proxy method: non-empty command tag is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})
}

func TestTxProxy_QueryRow(t *testing.T) {
	t.Run("it proxies method call when exec is done successfully", func(t *testing.T) {
		ctx := context.Background()
		expectedRow := &FakeRows{}

		innerTx := &intr.FakeTx{}
		innerTx.NextQueryRow = expectedRow

		tracker, events := eventTrack()

		testee := proxy.WrapTx(innerTx, events)

		row := testee.QueryRow(ctx, `select * from  "table" where "id" = $1;`, 42)

		if expectedRow != row {
			t.Error("it does not proxy method: wrong Row is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})
}

func TestTxProxy_Conn(t *testing.T) {
	t.Run("it proxies method call", func(t *testing.T) {
		expectedConn := &pgx.Conn{}

		innerTx := &intr.FakeTx{}
		innerTx.NextConn = expectedConn
		testee := proxy.WrapTx(innerTx, proxy.NewPgxEvents())

		conn := testee.Conn()

		if expectedConn != conn {
			t.Error("it does not proxy method: wrong conn is returned")
		}
	})

}

// ---
// Conn
// ---

func TestConnProxy_Begin(t *testing.T) {
	t.Run("it proxies method call when transaction is started succesfully", func(t *testing.T) {
		ctx := context.Background()
		tx := &intr.FakeTx{}

		innerConn := &intr.FakeConn{}
		innerConn.NextBegin.Tx = tx

		testee := proxy.WrapConn(innerConn, proxy.NewPgxEvents())

		actual, err := testee.Begin(ctx)

		if err != nil {
			t.Fatal("unexpected error")
		}

		txp, ok := actual.(*proxy.Tx)
		if !ok {
			t.Fatal("transaction type is unexpected one")
		}

		if txp.Base != tx {
			t.Error("it does not wrap transaction")
		}

		if txp.Events() != testee.Events() {
			t.Error("it does not pass events to transaction")
		}
	})

	t.Run("it proxies method call when transaction is not started", func(t *testing.T) {
		ctx := context.Background()
		errInBegin := errors.New("error")

		innerConn := &intr.FakeConn{}
		innerConn.NextBegin.Err = errInBegin

		testee := proxy.WrapConn(innerConn, proxy.NewPgxEvents())

		actual, err := testee.Begin(ctx)

		if err != errInBegin {
			t.Error("unexpected error")
		}

		if actual != nil {
			t.Error("unexpected transaction")
		}
	})
}

func TestConnProxy_BeginTx(t *testing.T) {
	t.Run("it proxies method call when transaction is started succesfully", func(t *testing.T) {
		ctx := context.Background()
		tx := &intr.FakeTx{}

		innerConn := &intr.FakeConn{}
		innerConn.NextBeginTx.Tx = tx

		testee := proxy.WrapConn(innerConn, proxy.NewPgxEvents())

		actual, err := testee.BeginTx(ctx, pgx.TxOptions{})

		if err != nil {
			t.Fatal("unexpected error")
		}

		txp, ok := actual.(*proxy.Tx)
		if !ok {
			t.Fatal("transaction type is unexpected one")
		}

		if txp.Base != tx {
			t.Error("it does not wrap transaction")
		}

		if txp.Events() != testee.Events() {
			t.Error("it does not pass events to transaction")
		}
	})

	t.Run("it proxies method call when transaction is not started", func(t *testing.T) {
		ctx := context.Background()
		errInBegin := errors.New("error")

		innerConn := &intr.FakeConn{}
		innerConn.NextBeginTx.Err = errInBegin

		testee := proxy.WrapConn(innerConn, proxy.NewPgxEvents())

		actual, err := testee.BeginTx(ctx, pgx.TxOptions{})

		if err != errInBegin {
			t.Error("unexpected error")
		}

		if actual != nil {
			t.Error("unexpected transaction")
		}
	})
}

func TestConnProxy_Exec(t *testing.T) {
	t.Run("it proxies method call when exec is done successfully", func(t *testing.T) {

		ctx := context.Background()
		commandTagInExec := pgconn.CommandTag([]byte("fake command tag"))

		innerConn := &intr.FakeConn{}
		innerConn.NextExec.CommandTag = commandTagInExec

		tracker, events := eventTrack()

		testee := proxy.WrapConn(innerConn, events)

		commandTag, err := testee.Exec(ctx, `update "table" ("column") values ($1);`, 42)

		if err != nil {
			t.Error("unexpected error is returned")
		}

		if !cmp.SliceEq(commandTag, commandTagInExec) {
			t.Error("it does not proxy method: wrong command tag is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})

	t.Run("it proxies method call when exec has failed", func(t *testing.T) {

		ctx := context.Background()
		errInExec := errors.New("error")

		innerConn := &intr.FakeConn{}
		innerConn.NextExec.Err = errInExec

		tracker, events := eventTrack()

		testee := proxy.WrapConn(innerConn, events)

		commandTag, err := testee.Exec(ctx, `update "table" ("column") values ($1);`, 42)

		if err != errInExec {
			t.Error("unexpected error is returned")
		}

		if !cmp.SliceEq(commandTag, pgconn.CommandTag([]byte{})) {
			t.Error("it does not proxy method: non-empty command tag is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})
}

func TestConnProxy_Query(t *testing.T) {
	t.Run("it proxies method call when exec is done successfully", func(t *testing.T) {

		ctx := context.Background()
		expectedRows := &FakeRows{}

		innerConn := &intr.FakeConn{}
		innerConn.NextQuery.Rows = expectedRows

		tracker, events := eventTrack()

		testee := proxy.WrapConn(innerConn, events)

		rows, err := testee.Query(ctx, `select * from  "table" where "column" = $1;`, 42)

		if err != nil {
			t.Error("unexpected error is returned")
		}

		if expectedRows != rows {
			t.Error("it does not proxy method: wrong Rows are returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})

	t.Run("it proxies method call when exec has failed", func(t *testing.T) {

		ctx := context.Background()
		expectedErr := errors.New("error")

		innerConn := &intr.FakeConn{}
		innerConn.NextQuery.Err = expectedErr

		tracker, events := eventTrack()

		testee := proxy.WrapConn(innerConn, events)

		row, err := testee.Query(ctx, `select * from "table" where "column" = $1;`, 42)

		if err != expectedErr {
			t.Error("unexpected error is returned")
		}

		if row != nil {
			t.Error("it does not proxy method: non-empty command tag is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})
}

func TestConnProxy_QueryRow(t *testing.T) {
	t.Run("it proxies method call when exec is done successfully", func(t *testing.T) {
		ctx := context.Background()
		expectedRow := &FakeRows{}

		innerConn := &intr.FakeConn{}
		innerConn.NextQueryRow = expectedRow

		tracker, events := eventTrack()

		testee := proxy.WrapConn(innerConn, events)

		row := testee.QueryRow(ctx, `select * from  "table" where "id" = $1;`, 42)

		if expectedRow != row {
			t.Error("it does not proxy method: wrong Row is returned")
		}

		if !cmp.SliceEq(tracker.timeline, []eventType{beforeQuery, afterQuery}) {
			t.Errorf("invoked events are wrong: %v", tracker.timeline)
		}
	})
}

func TestConnProxy_Conn(t *testing.T) {
	t.Run("it proxies to the inner pool", func(t *testing.T) {
		conn := &pgx.Conn{}

		inner := &intr.FakeConn{}
		inner.NextConn = conn
		testee := proxy.WrapConn(inner, proxy.NewPgxEvents())

		if testee.Conn() != conn {
			t.Error("it does not proxy to the inner object.")
		}
	})
}

func TestConnProxy_Ping(t *testing.T) {
	t.Run("it proxies to the inner pool", func(t *testing.T) {
		pingErr := errors.New("no pongs")

		inner := &intr.FakeConn{}
		inner.NextPing = pingErr

		testee := proxy.WrapConn(inner, proxy.NewPgxEvents())
		if testee.Ping(context.Background()) != pingErr {
			t.Error("it does not proxy to the inner object.")
		}
	})
}
