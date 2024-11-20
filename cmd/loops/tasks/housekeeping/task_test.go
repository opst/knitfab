package housekeeping_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/cmd/loops/tasks/housekeeping"
	"github.com/opst/knitfab/pkg/domain"
	dbdatamocks "github.com/opst/knitfab/pkg/domain/data/db/mock"
	"github.com/opst/knitfab/pkg/domain/data/k8s/dataagt"
	k8sdatamocks "github.com/opst/knitfab/pkg/domain/data/k8s/mock"
	"github.com/opst/knitfab/pkg/domain/knitfab/k8s/cluster"
)

type MockDataAgent struct {
	name       string
	aPIPort    int32
	url        string
	mode       domain.DataAgentMode
	knitID     string
	volumeRef  string
	podPhase   cluster.PodPhase
	stringExpr string
	closeError error
	closed     bool
}

func (m *MockDataAgent) Name() string {
	return m.name
}

func (m *MockDataAgent) APIPort() int32 {
	return m.aPIPort
}

func (m *MockDataAgent) URL() string {
	return m.url
}

func (m *MockDataAgent) Mode() domain.DataAgentMode {
	return m.mode
}

func (m *MockDataAgent) KnitID() string {
	return m.knitID
}

func (m *MockDataAgent) VolumeRef() string {
	return m.volumeRef
}

func (m *MockDataAgent) PodPhase() cluster.PodPhase {
	return m.podPhase
}

func (m *MockDataAgent) String() string {
	return m.stringExpr
}

func (m *MockDataAgent) Close() error {
	m.closed = true
	return m.closeError
}

type CallbackReturns struct {
	RemoveOk bool
	Err      error
}

type PickAndRemoveAgentReturns struct {
	Cursor domain.DataAgentCursor
	Err    error
}

type TaskReturns struct {
	Cursor domain.DataAgentCursor
	Ok     bool
	Err    error
}

func (r TaskReturns) Satisfies(other TaskReturns) bool {
	return r.Cursor == other.Cursor &&
		r.Ok == other.Ok &&
		errors.Is(r.Err, other.Err)
}

func TestTask(t *testing.T) {
	{
		type When struct {
			Cursor                    domain.DataAgentCursor
			PickAndRemoveAgentReturns PickAndRemoveAgentReturns
		}
		type Then struct {
			TaskReturns TaskReturns
		}
		theory := func(when When, then Then) func(t *testing.T) {
			return func(t *testing.T) {

				mDataInterface := dbdatamocks.NewDataInterface()
				mDataInterface.Impl.PickAndRemoveAgent = func(
					_ context.Context, cursor domain.DataAgentCursor,
					_ func(domain.DataAgent) (bool, error),
				) (domain.DataAgentCursor, error) {
					if cursor != when.Cursor {
						t.Errorf("expected cursor %v, got %v", when.Cursor, cursor)
					}
					r := when.PickAndRemoveAgentReturns
					return r.Cursor, r.Err
				}

				testee := housekeeping.Task(mDataInterface, nil)

				ctx := context.Background()

				actualCursor, actualOk, actualErr := testee(ctx, when.Cursor)
				actual := TaskReturns{
					Cursor: actualCursor, Ok: actualOk, Err: actualErr,
				}

				if !actual.Satisfies(then.TaskReturns) {
					t.Errorf(
						"expected task returns %+v, got %+v",
						then.TaskReturns, actual,
					)
				}
			}
		}

		{
			cursor := domain.DataAgentCursor{Head: ""}
			expectedErr := errors.New("fake error")
			t.Run("should return error if PickAndRemoveAgent returns error", theory(
				When{
					Cursor: cursor,
					PickAndRemoveAgentReturns: PickAndRemoveAgentReturns{
						Cursor: cursor, Err: expectedErr,
					},
				},
				Then{
					TaskReturns: TaskReturns{
						Cursor: cursor, Ok: false, Err: expectedErr,
					},
				},
			))
		}
		{
			givenCursor := domain.DataAgentCursor{Head: ""}
			returnedCursor := domain.DataAgentCursor{Head: "some dataagent name"}
			t.Run("should return ok if PickAndRemoveAgent returns new cursor", theory(
				When{
					Cursor: givenCursor,
					PickAndRemoveAgentReturns: PickAndRemoveAgentReturns{
						Cursor: returnedCursor, Err: nil,
					},
				},
				Then{
					TaskReturns: TaskReturns{
						Cursor: returnedCursor, Ok: true, Err: nil,
					},
				},
			))
		}
		{
			givenCursor := domain.DataAgentCursor{Head: ""}
			t.Run("should return false if PickAndRemoveAgent returns same cursor", theory(
				When{
					Cursor: givenCursor,
					PickAndRemoveAgentReturns: PickAndRemoveAgentReturns{
						Cursor: givenCursor, Err: nil,
					},
				},
				Then{
					TaskReturns: TaskReturns{
						Cursor: givenCursor, Ok: false, Err: nil,
					},
				},
			))
		}
	}

	{
		type When struct {
			InvokeCallbackWith domain.DataAgent
		}
		theory := func(when When) func(t *testing.T) {
			return func(t *testing.T) {
				mDataInterface := dbdatamocks.NewDataInterface()
				mDataInterface.Impl.PickAndRemoveAgent = func(
					_ context.Context, _ domain.DataAgentCursor,
					f func(domain.DataAgent) (bool, error),
				) (domain.DataAgentCursor, error) {
					f(when.InvokeCallbackWith)
					return domain.DataAgentCursor{}, nil
				}

				mockIData := k8sdatamocks.New(t)
				mockIData.Impl.FindDataAgent = func(
					ctx context.Context, da domain.DataAgent,
				) (dataagt.DataAgent, error) {
					if da.Name != when.InvokeCallbackWith.Name {
						t.Errorf("expected name %v, got %v", when.InvokeCallbackWith.Name, da.Name)
					}

					return &MockDataAgent{
						podPhase: cluster.PodSucceeded,
					}, nil
				}

				testee := housekeeping.Task(mDataInterface, mockIData)

				ctx := context.Background()
				testee(ctx, domain.DataAgentCursor{})
			}
		}

		t.Run("should call GetPod with data agent name", theory(
			When{
				InvokeCallbackWith: domain.DataAgent{Name: "fake name"},
			},
		))
	}

	{
		type When struct {
			PodStatus      cluster.PodPhase
			PodError       error
			PodCloseResult error
		}
		type Then struct {
			Error           error
			WantClose       bool
			CallbackReturns CallbackReturns
		}
		theory := func(when When, then Then) func(t *testing.T) {
			return func(t *testing.T) {
				mDataInterface := dbdatamocks.NewDataInterface()
				mDataInterface.Impl.PickAndRemoveAgent = func(
					_ context.Context, _ domain.DataAgentCursor,
					f func(domain.DataAgent) (bool, error),
				) (domain.DataAgentCursor, error) {
					ok, err := f(domain.DataAgent{Name: "fake name"})
					if ok != then.CallbackReturns.RemoveOk {
						t.Errorf(
							"callback ok: got %v, expected %v",
							ok, then.CallbackReturns.RemoveOk,
						)
					}
					if !errors.Is(err, then.CallbackReturns.Err) {
						t.Errorf(
							"callback err: got %v, expected %v",
							err, then.CallbackReturns.Err,
						)
					}
					return domain.DataAgentCursor{}, nil
				}

				mockDataAgent := &MockDataAgent{
					podPhase:   when.PodStatus,
					closeError: when.PodCloseResult,
				}

				mGetPodder := k8sdatamocks.New(t)
				mGetPodder.Impl.FindDataAgent = func(
					ctx context.Context, da domain.DataAgent,
				) (dataagt.DataAgent, error) {
					return mockDataAgent, when.PodError
				}

				testee := housekeeping.Task(mDataInterface, mGetPodder)

				ctx := context.Background()
				testee(ctx, domain.DataAgentCursor{})

				if mockDataAgent.closed != then.WantClose {
					t.Errorf(
						"pod is closed?: actual = %v , expected = %v",
						mockDataAgent.closed, then.WantClose,
					)
				}
			}
		}

		t.Run("should close pod if pod status is succeeded", theory(
			When{PodStatus: cluster.PodSucceeded},
			Then{
				WantClose: true,
				CallbackReturns: CallbackReturns{
					RemoveOk: true, Err: nil,
				},
			},
		))

		t.Run("should close pod if pod status is failed", theory(
			When{PodStatus: cluster.PodFailed},
			Then{
				WantClose: true,
				CallbackReturns: CallbackReturns{
					RemoveOk: true, Err: nil,
				},
			},
		))

		t.Run("should close pod if pod status is stucking", theory(
			When{PodStatus: cluster.PodStucking},
			Then{
				WantClose: true,
				CallbackReturns: CallbackReturns{
					RemoveOk: true, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is pending", theory(
			When{PodStatus: cluster.PodPending},
			Then{
				WantClose: false,
				CallbackReturns: CallbackReturns{
					RemoveOk: false, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is running", theory(
			When{PodStatus: cluster.PodRunning},
			Then{
				WantClose: false,
				CallbackReturns: CallbackReturns{
					RemoveOk: false, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is unknown", theory(
			When{PodStatus: cluster.PodUnknown},
			Then{
				WantClose: false,
				CallbackReturns: CallbackReturns{
					RemoveOk: false, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is empty", theory(
			When{PodStatus: ""},
			Then{
				WantClose: false,
				CallbackReturns: CallbackReturns{
					RemoveOk: false, Err: nil,
				},
			},
		))

		{
			expectedErr := errors.New("fake error")
			t.Run("should not close pod if getting pod is error", theory(
				When{PodStatus: cluster.PodFailed, PodError: expectedErr},
				Then{
					WantClose: false,
					CallbackReturns: CallbackReturns{
						RemoveOk: false, Err: expectedErr,
					},
				},
			))
		}

		{
			expectedErr := errors.New("fake error")
			t.Run("should not close pod if pod closing is error", theory(
				When{PodStatus: cluster.PodFailed, PodCloseResult: expectedErr},
				Then{
					WantClose: true,
					CallbackReturns: CallbackReturns{
						RemoveOk: false, Err: expectedErr,
					},
				},
			))
		}
	}
}
