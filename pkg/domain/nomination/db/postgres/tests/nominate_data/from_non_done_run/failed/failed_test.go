package failed_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/domain/nomination/db/postgres/tests/nominate_data/from_non_done_run/internal/testcases"
)

func TestNominator_NominateData_Nominate_Failed(t *testing.T) {
	for name, testcase := range testcases.GenearteTestcasesFor(domain.Failed) {
		t.Run(name, testcases.Theory(testcase))
	}
}
