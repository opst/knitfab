package failed_test

import (
	"context"
	"testing"

	"github.com/opst/knitfab/pkg/conn/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/nomination/db/postgres/tests/nominate_data/from_non_done_run/internal/testcases"
	"github.com/opst/knitfab/pkg/domain/nomination/db/postgres/tests/nominate_data/internal/dataset"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestNominator_NominateData_Nominate_Failed(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	ctx := context.Background()
	pool := poolBroaker.GetPool(ctx, t)

	rootTx := try.To(pool.Begin(ctx)).OrFatal(t)
	defer rootTx.Rollback(ctx)

	func() {
		tx := try.To(rootTx.Begin(ctx)).OrFatal(t)
		defer tx.Rollback(ctx)
		if err := dataset.GivenDatabase.ApplyWithConn(ctx, tx); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	for name, testcase := range testcases.GenearteTestcasesFor(domain.Failed) {
		t.Run(name, testcases.Theory(testcase, rootTx, pool))
	}
}
