package schema_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	kubeerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	dbtestenv "github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/schema"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/pointer"
	"github.com/opst/knitfab/pkg/utils/try"
	k8stestenv "github.com/opst/knitfab/pkg/workloads/k8s/testenv"
)

func TestPgSchema_Upgrade(t *testing.T) {
	type When struct {
		Testdata string
	}

	type Then struct {
		VersionBefore               int
		TableSchemaVersionNotExists bool
		TableSchemaVersion          []schemaVersionTable
		VersionAfter                int

		TableFooNotExists bool
		TableFoo          []exampleTable

		TableBarNotExists bool
		TableBar          []exampleTable
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			{
				dl, ok := t.Deadline()
				if ok {
					_ctx, cancel := context.WithDeadline(ctx, dl.Add(-1*time.Second))
					defer cancel()
					ctx = _ctx
				}
			}
			pool := startPostgresDatabase(ctx, t).GetPool(ctx, t)

			if err := func() error {
				given, err := os.ReadFile(filepath.Join(when.Testdata, "given.sql"))
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						return nil
					}
					return err
				}

				if len(given) == 0 {
					return nil
				}

				tx := try.To(pool.Begin(ctx)).OrFatal(t)
				defer tx.Rollback(ctx)

				if _, err := tx.Exec(ctx, string(given)); err != nil {
					return err
				}
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				return nil
			}(); err != nil {
				t.Fatalf("failed to setup database: %v", err)
			}

			testee := schema.New(pool, filepath.Join(when.Testdata, "versions"))
			{
				got := try.To(testee.Version(ctx)).OrFatal(t)
				if got != then.VersionBefore {
					t.Errorf("version before upgrade\n- got: %v\n- want: %v", got, then.VersionBefore)
				}
			}

			if err := testee.Upgrade(ctx); err != nil {
				t.Fatalf("failed to upgrade schema: %v", err)
			}

			{
				got := try.To(testee.Version(ctx)).OrFatal(t)
				if got != then.VersionAfter {
					t.Errorf("version after upgrade\n- got: %v\n- want: %v", got, then.VersionAfter)
				}
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			{
				gotVersion, err := scanner.New[schemaVersionTable]().QueryAll(
					ctx, conn, `table "schema_version"`,
				)
				if err != nil {
					pgerr := new(pgconn.PgError)
					if !errors.As(err, &pgerr) || !then.TableSchemaVersionNotExists || pgerr.Code != pgerrcode.UndefinedTable {
						t.Fatal(err)
					}
				}

				if !cmp.SliceContentEq(gotVersion, then.TableSchemaVersion) {
					t.Errorf(
						"table schema_version\n- got: %v\n- want: %v",
						gotVersion, then.TableSchemaVersion,
					)
				}
			}
			{
				gotFoo, err := scanner.New[exampleTable]().QueryAll(
					ctx, conn, `table "foo"`,
				)
				if err != nil {
					pgerr := new(pgconn.PgError)
					if !errors.As(err, &pgerr) || !then.TableFooNotExists || pgerr.Code != pgerrcode.UndefinedTable {
						t.Fatal(err)
					}
				}
				if !cmp.SliceContentEq(gotFoo, then.TableFoo) {
					t.Errorf(
						"table foo\n- got: %v\n- want: %v",
						gotFoo, then.TableFoo,
					)
				}
			}

			{
				gotBar, err := scanner.New[exampleTable]().QueryAll(
					ctx, conn, `table "bar"`,
				)
				if err != nil {
					pgerr := new(pgconn.PgError)
					if !errors.As(err, &pgerr) || !then.TableBarNotExists || pgerr.Code != pgerrcode.UndefinedTable {
						t.Fatal(err)
					}
				}
				if !cmp.SliceContentEq(gotBar, then.TableBar) {
					t.Errorf(
						"table bar\n- got: %v\n- want: %v",
						gotBar, then.TableBar,
					)
				}
			}
		}
	}

	t.Run("case 1: build schema from scratch", theory(
		When{
			Testdata: "testdata/case1",
		},
		Then{
			VersionBefore: 0,
			TableSchemaVersion: []schemaVersionTable{
				{Version: 2},
			},
			VersionAfter: 2,

			TableFoo: []exampleTable{
				{Id: 1, Name: "foo-1"},
				{Id: 2, Name: "foo-2"},
			},

			TableBar: []exampleTable{
				{Id: 1, Name: "bar-1"},
			},
		},
	))

	t.Run("case 2: upgrade schema from version 1 to 2", theory(
		When{
			Testdata: "testdata/case2",
		},
		Then{
			VersionBefore: 1,
			TableSchemaVersion: []schemaVersionTable{
				{Version: 2},
			},
			VersionAfter: 2,

			TableFoo: []exampleTable{
				{Id: 1, Name: "foo-1"},
				{Id: 2, Name: "foo-2"},
			},
			TableBar: []exampleTable{
				{Id: 1, Name: "bar-1"},
			},
		},
	))

	t.Run("case 3: no upgrade", theory(
		When{
			Testdata: "testdata/case3",
		},

		Then{
			VersionBefore: 2,
			TableSchemaVersion: []schemaVersionTable{
				{Version: 2},
			},
			VersionAfter: 2,

			TableFooNotExists: true,
			TableBarNotExists: true,
		},
	))
}

func TestSchema_Context(t *testing.T) {
	ctx := context.Background()
	{
		dl, ok := t.Deadline()
		if ok {
			_ctx, cancel := context.WithDeadline(ctx, dl.Add(-1*time.Second))
			defer cancel()
			ctx = _ctx
		}
	}
	pool := startPostgresDatabase(ctx, t).GetPool(ctx, t)

	// step1. if there are no schema_version table, context should be canceled.
	func() {
		testee := schema.New(pool, "testdata/case4/versions")
		schema_ctx, cancel := testee.Context(ctx)
		defer cancel()

		<-schema_ctx.Done()
		if err := schema_ctx.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	if err := func() error {
		tx := try.To(pool.Begin(ctx)).OrFatal(t)
		defer tx.Rollback(ctx)
		try.To(tx.Exec(
			ctx,
			`
			CREATE TABLE "schema_version" (
				"version" int not null,
				primary key ("version")
			);
			INSERT INTO "schema_version" ("version") VALUES (1);
			`,
		)).OrFatal(t)
		return tx.Commit(ctx)
	}(); err != nil {
		t.Fatal(err)
	}

	// step2. if the schema is same version as the requirement, context should not be canceled.
	func() {
		testee := schema.New(pool, "testdata/case4/versions")
		schema_ctx, cancel := testee.Context(ctx)
		defer cancel()

		select {
		case <-schema_ctx.Done():
			t.Errorf("unexpected cancelation")
		default:
		}
		if err := schema_ctx.Err(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// step3. if the schema is older than the requirement, context should be canceled.
	func() {
		testee := schema.New(pool, "testdata/case1/versions")
		schema_ctx, cancel := testee.Context(ctx)
		defer cancel()

		<-schema_ctx.Done()
		if err := schema_ctx.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// step4. if the requirement is updated and the schema is older than the requirement, context should be canceled.
	func() {
		dir := t.TempDir()
		os.Mkdir(filepath.Join(dir, "1"), 0755)

		testee := schema.New(pool, dir)
		schema_ctx, cancel := testee.Context(ctx)
		defer cancel()

		select {
		case <-schema_ctx.Done():
			t.Errorf("unexpected cancelation")
		default:
		}
		if err := schema_ctx.Err(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if err := os.Mkdir(filepath.Join(dir, "2"), 0755); err != nil {
			t.Fatal(err)
		}

		<-schema_ctx.Done()
		if err := schema_ctx.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v", err)
		}
	}()
}

type schemaVersionTable struct {
	Version int
}

type exampleTable struct {
	Id   int
	Name string
}

func startPostgresDatabase(ctx context.Context, t *testing.T) dbtestenv.PoolBroaker {
	DBPOD_NAME := "test-db"
	PASSWORD := "password"
	USER := "user"
	DBNAME := "test"

	NAMESPACE := k8stestenv.Namespace()
	k8s := k8stestenv.NewClient()
	t.Cleanup(func() {
		k8s.CoreV1().Pods(NAMESPACE).Delete(
			context.Background(), DBPOD_NAME,
			metav1.DeleteOptions{GracePeriodSeconds: pointer.Ref(int64(0))},
		)
	})

	if err := k8s.CoreV1().Pods(NAMESPACE).Delete(
		ctx, DBPOD_NAME,
		metav1.DeleteOptions{GracePeriodSeconds: pointer.Ref(int64(0))},
	); err != nil {
		if !kubeerr.IsNotFound(err) {
			t.Fatalf("failed to delete pod: %v", err)
		}
	}

	try.To(k8s.CoreV1().Pods(NAMESPACE).Create(
		ctx,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: DBPOD_NAME,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "postgres",
						Image: "postgres:15.6-bullseye",
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"psql", "-U", USER, "-d", "test", "-c", "SELECT 1",
									},
								},
							},
							InitialDelaySeconds: 3,
							PeriodSeconds:       1,
						},
						Env: []corev1.EnvVar{
							{
								Name:  "POSTGRES_PASSWORD",
								Value: PASSWORD,
							},
							{
								Name:  "POSTGRES_USER",
								Value: USER,
							},
							{
								Name:  "POSTGRES_DB",
								Value: DBNAME,
							},
						},
						Ports: []corev1.ContainerPort{
							{
								Name:          "postgres",
								ContainerPort: 5432,
							},
						},
					},
				},
			},
		},
		metav1.CreateOptions{},
	)).OrFatal(t)

	var dbpod *corev1.Pod
WAIT_POD:
	for {
		pod, err := k8s.CoreV1().Pods(NAMESPACE).Get(ctx, DBPOD_NAME, metav1.GetOptions{})
		if err != nil && !kubeerr.IsNotFound(err) {
			t.Fatalf("failed to get pod: %v", err)
		}
		if pod != nil {
			for _, c := range pod.Status.Conditions {
				if c.Status == corev1.ConditionTrue && c.Type == corev1.PodReady {
					dbpod = pod
					break WAIT_POD
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
			return nil
		case <-time.After(1 * time.Second):
			continue
		}
	}

	pf := try.To(k8stestenv.PortforwardWithPod(
		ctx, dbpod, 5432, k8stestenv.WithLog(t),
	)).OrFatal(t)
	poolb := dbtestenv.NewPoolBroakerWithForwarder(
		ctx, t, pf,
		dbtestenv.WithPassword(PASSWORD),
		dbtestenv.WithUser(USER),
		dbtestenv.WithDbname(DBNAME),
		dbtestenv.WithDoNotCleanup(),
	)
	return poolb
}
