package housekeeping_test

import (
	"context"
	"errors"
	"testing"

	"github.com/opst/knitfab/cmd/loops/tasks/housekeeping"
	kdb "github.com/opst/knitfab/pkg/db"
	"github.com/opst/knitfab/pkg/db/mocks"
	"github.com/opst/knitfab/pkg/utils/retry"
	"github.com/opst/knitfab/pkg/workloads/k8s"
	kubecore "k8s.io/api/core/v1"
	kubeevents "k8s.io/api/events/v1"
)

type MockedGetPodder struct {
	ImplGetPod func(
		ctx context.Context, backoff retry.Backoff, name string,
		requirements ...k8s.Requirement[k8s.WithEvents[*kubecore.Pod]],
	) retry.Promise[k8s.Pod]
}

func (m *MockedGetPodder) GetPod(
	ctx context.Context, backoff retry.Backoff, name string,
	requirements ...k8s.Requirement[k8s.WithEvents[*kubecore.Pod]],
) retry.Promise[k8s.Pod] {
	return m.ImplGetPod(ctx, backoff, name, requirements...)
}

type MockPod struct {
	MockedStatus k8s.PodPhase
	IsClosed     bool
	CloseResult  error
}

var _ k8s.Pod = &MockPod{}

func (m *MockPod) Name() string {
	return "fake name"
}

func (m *MockPod) Host() string {
	return "test.invalid"
}

func (m *MockPod) Ports() map[string]int32 {
	return map[string]int32{"http": 8080}
}

func (m *MockPod) Namespace() string {
	return "example"
}

func (m *MockPod) Status() k8s.PodPhase {
	return m.MockedStatus
}

func (m *MockPod) Close() error {
	m.IsClosed = true
	return m.CloseResult
}

func (m *MockPod) Events() []kubeevents.Event {
	return nil
}

type CallbackReturns struct {
	RemoveOk bool
	Err      error
}

type PickAndRemoveAgentReturns struct {
	Cursor kdb.DataAgentCursor
	Err    error
}

type TaskReturns struct {
	Cursor kdb.DataAgentCursor
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
			Cursor                    kdb.DataAgentCursor
			PickAndRemoveAgentReturns PickAndRemoveAgentReturns
		}
		type Then struct {
			TaskReturns TaskReturns
		}
		theory := func(when When, then Then) func(t *testing.T) {
			return func(t *testing.T) {

				mDataInterface := mocks.NewDataInterface()
				mDataInterface.Impl.PickAndRemoveAgent = func(
					_ context.Context, cursor kdb.DataAgentCursor,
					_ func(kdb.DataAgent) (bool, error),
				) (kdb.DataAgentCursor, error) {
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
			cursor := kdb.DataAgentCursor{Head: ""}
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
			givenCursor := kdb.DataAgentCursor{Head: ""}
			returnedCursor := kdb.DataAgentCursor{Head: "some dataagent name"}
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
			givenCursor := kdb.DataAgentCursor{Head: ""}
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
			InvokeCallbackWith kdb.DataAgent
		}
		theory := func(when When) func(t *testing.T) {
			return func(t *testing.T) {
				mDataInterface := mocks.NewDataInterface()
				mDataInterface.Impl.PickAndRemoveAgent = func(
					_ context.Context, _ kdb.DataAgentCursor,
					f func(kdb.DataAgent) (bool, error),
				) (kdb.DataAgentCursor, error) {
					f(when.InvokeCallbackWith)
					return kdb.DataAgentCursor{}, nil
				}

				mGetPodder := &MockedGetPodder{
					ImplGetPod: func(
						ctx context.Context, backoff retry.Backoff, name string,
						_ ...k8s.Requirement[k8s.WithEvents[*kubecore.Pod]],
					) retry.Promise[k8s.Pod] {

						if name != when.InvokeCallbackWith.Name {
							t.Errorf("expected name %v, got %v", when.InvokeCallbackWith.Name, name)
						}

						ch := make(chan retry.Result[k8s.Pod], 1)
						ch <- retry.Result[k8s.Pod]{
							Value: &MockPod{MockedStatus: k8s.PodSucceeded},
						}
						close(ch)
						return ch
					},
				}

				testee := housekeeping.Task(mDataInterface, mGetPodder)

				ctx := context.Background()
				testee(ctx, kdb.DataAgentCursor{})
			}
		}

		t.Run("should call GetPod with data agent name", theory(
			When{
				InvokeCallbackWith: kdb.DataAgent{Name: "fake name"},
			},
		))
	}

	{
		type When struct {
			PodStatus      k8s.PodPhase
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
				mDataInterface := mocks.NewDataInterface()
				mDataInterface.Impl.PickAndRemoveAgent = func(
					_ context.Context, _ kdb.DataAgentCursor,
					f func(kdb.DataAgent) (bool, error),
				) (kdb.DataAgentCursor, error) {
					ok, err := f(kdb.DataAgent{Name: "fake name"})
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
					return kdb.DataAgentCursor{}, nil
				}

				pod := &MockPod{
					MockedStatus: when.PodStatus,
					CloseResult:  when.PodCloseResult,
				}

				mGetPodder := &MockedGetPodder{
					ImplGetPod: func(
						ctx context.Context, backoff retry.Backoff, name string,
						_ ...k8s.Requirement[k8s.WithEvents[*kubecore.Pod]],
					) retry.Promise[k8s.Pod] {
						ch := make(chan retry.Result[k8s.Pod], 1)
						ch <- retry.Result[k8s.Pod]{
							Value: pod, Err: when.PodError,
						}
						close(ch)
						return ch
					},
				}

				testee := housekeeping.Task(mDataInterface, mGetPodder)

				ctx := context.Background()
				testee(ctx, kdb.DataAgentCursor{})

				if pod.IsClosed != then.WantClose {
					t.Errorf(
						"pod is closed?: actual = %v , expected = %v",
						pod.IsClosed, then.WantClose,
					)
				}
			}
		}

		t.Run("should close pod if pod status is succeeded", theory(
			When{PodStatus: k8s.PodSucceeded},
			Then{
				WantClose: true,
				CallbackReturns: CallbackReturns{
					RemoveOk: true, Err: nil,
				},
			},
		))

		t.Run("should close pod if pod status is failed", theory(
			When{PodStatus: k8s.PodFailed},
			Then{
				WantClose: true,
				CallbackReturns: CallbackReturns{
					RemoveOk: true, Err: nil,
				},
			},
		))

		t.Run("should close pod if pod status is stucking", theory(
			When{PodStatus: k8s.PodStucking},
			Then{
				WantClose: true,
				CallbackReturns: CallbackReturns{
					RemoveOk: true, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is pending", theory(
			When{PodStatus: k8s.PodPending},
			Then{
				WantClose: false,
				CallbackReturns: CallbackReturns{
					RemoveOk: false, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is running", theory(
			When{PodStatus: k8s.PodRunning},
			Then{
				WantClose: false,
				CallbackReturns: CallbackReturns{
					RemoveOk: false, Err: nil,
				},
			},
		))

		t.Run("should not close pod if pod status is unknown", theory(
			When{PodStatus: k8s.PodUnknown},
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
				When{PodStatus: k8s.PodFailed, PodError: expectedErr},
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
				When{PodStatus: k8s.PodFailed, PodCloseResult: expectedErr},
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
