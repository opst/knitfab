package tests_test

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgnommock "github.com/opst/knitfab/pkg/db/postgres/nominator/mock"
	"github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	kpgrun "github.com/opst/knitfab/pkg/db/postgres/run"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	"github.com/opst/knitfab/pkg/db/postgres/tables/matcher"
	th "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils/try"
)

type mockRunNamingConvention struct {
	test *testing.T
	impl struct {
		VolumeRef func(string) (string, error)
		Worker    func(string) (string, error)
	}
}

func newMockRunNamingConvention(t *testing.T) mockRunNamingConvention {
	return mockRunNamingConvention{test: t}
}

func (m mockRunNamingConvention) VolumeRef(knitId string) (string, error) {
	m.test.Helper()
	if m.impl.VolumeRef == nil {
		m.test.Fatalf("unexpected invokation: VolumeRef: with (%s)", knitId)
	}
	return m.impl.VolumeRef(knitId)
}
func (m mockRunNamingConvention) Worker(runId string) (string, error) {
	m.test.Helper()
	if m.impl.Worker == nil {
		m.test.Fatalf("unexpected invokation: Worker: with (%s)", runId)
	}
	return m.impl.Worker(runId)
}

func TestRun_NewPseudo(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	original := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(original)

	type When struct {
		planName     kdb.PseudoPlanName
		volumeRef    string
		volumeRefErr error
	}

	type Then struct {
		err      error
		planId   string
		outputId int
	}

	const (
		planUploaded = kdb.Uploaded
		planImport   = kdb.PseudoPlanName("knit#import")
	)

	planIds := map[kdb.PseudoPlanName]string{
		planUploaded: th.Padding36("pseudo-plan-1"),
		planImport:   th.Padding36("pseudo-plan-2"),
	}

	initialState := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: planIds[planUploaded], Active: true, Hash: th.Padding64(planUploaded).String()},
			{PlanId: planIds[planImport], Active: true, Hash: th.Padding64(planImport).String()},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: planIds[planUploaded], Name: string(planUploaded)},
			{PlanId: planIds[planImport], Name: string(planImport)},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1, PlanId: planIds[planUploaded], Path: "/out"}: {},
			{OutputId: 2, PlanId: planIds[planImport], Path: "/out"}:   {},
		},
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pgpool := poolBroaker.GetPool(ctx, t)
			if err := initialState.Apply(ctx, pgpool); err != nil {
				t.Fatal(err)
			}

			nom := kpgnommock.New(t)
			naming := newMockRunNamingConvention(t)
			naming.impl.VolumeRef = func(s string) (string, error) {
				return when.volumeRef, when.volumeRefErr
			}

			testee := kpgrun.New(
				pgpool,
				kpgrun.WithNominator(nom), kpgrun.WithNamingConvention(naming),
			)

			conn := try.To(pgpool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			lifecycleSuspend := 42 * time.Minute
			beforeNewPseudo := try.To(th.PGNow(ctx, conn)).OrFatal(t)
			actualRunId, err := testee.NewPseudo(ctx, when.planName, lifecycleSuspend)
			afterNewPseudo := try.To(th.PGNow(ctx, conn)).OrFatal(t)

			if !errors.Is(err, then.err) {
				t.Errorf("expected error does not occured: got %v, want: %v", err, then.err)

			}
			if then.err != nil {
				actual := try.To(scanner.New[tables.Run]().QueryAll(
					ctx, conn, `table "run"`,
				)).OrFatal(t)

				if len(actual) != 0 {
					t.Errorf("unexpected run is created: %+v", actual)
				}

				return
			}

			{
				expected := []matcher.Run{
					{
						RunId:                 matcher.EqEq(actualRunId),
						PlanId:                matcher.EqEq(planIds[when.planName]),
						Status:                matcher.EqEq(kdb.Running),
						UpdatedAt:             matcher.Between(beforeNewPseudo, afterNewPseudo),
						LifecycleSuspendUntil: matcher.After(beforeNewPseudo.Add(lifecycleSuspend)),
					},
				}
				actual := try.To(scanner.New[tables.Run]().QueryAll(
					ctx, conn, `table "run"`,
				)).OrFatal(t)

				if !cmp.SliceContentEqWith(expected, actual, matcher.Run.Match) {
					t.Errorf(
						"unmatch: run\n=== actual ===\n%+v\n=== expected ===\n%+v",
						actual, expected,
					)
				}
			}

			{
				expected := []matcher.Data{
					{
						KnitId:    matcher.Any[string](),
						VolumeRef: matcher.EqEq(when.volumeRef),
						RunId:     matcher.EqEq(actualRunId),
						OutputId:  matcher.EqEq(then.outputId),
						PlanId:    matcher.EqEq(planIds[when.planName]),
					},
				}
				actual := try.To(scanner.New[tables.Data]().QueryAll(
					ctx, conn, `table "data"`,
				)).OrFatal(t)

				if !cmp.SliceContentEqWith(expected, actual, matcher.Data.Match) {
					t.Errorf(
						"unmatch: data\n=== actual ===\n%+v\n=== expected ===\n%+v",
						actual, expected,
					)
				}
			}
		}
	}

	expectedVolumeRef := "pvc-name-1"
	t.Run("initialize with Uploaded creates new Upload Run", theory(
		When{
			planName:  kdb.Uploaded,
			volumeRef: expectedVolumeRef,
		},
		Then{
			planId:   planIds[kdb.Uploaded],
			outputId: 1,
		},
	))

	t.Run("when undefined plan name is passed, knitId should not be reserved", theory(
		When{
			planName:  kdb.PseudoPlanName("no such plan name"),
			volumeRef: expectedVolumeRef,
		},
		Then{
			err: kdb.ErrMissing,
		},
	))

	expectedError := errors.New("expected error")
	t.Run("when volumeRef generator causes error, knitId should not be reserved", theory(
		When{
			planName:     kdb.Uploaded,
			volumeRef:    "",
			volumeRefErr: expectedError,
		},
		Then{
			err: expectedError,
		},
	))
}
