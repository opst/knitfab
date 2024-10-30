package k8s_test

import (
	"strings"
	"testing"

	"github.com/opst/knitfab/pkg/utils/cmp"
	k8s "github.com/opst/knitfab/pkg/workloads/k8s"
)

type FakeSelector string

func (fs FakeSelector) QueryString(key string) string {
	return key + ":" + string(fs)
}

func (fs FakeSelector) Equal(s k8s.SelectorElement) bool {
	switch t := s.(type) {
	case FakeSelector:
		return t == fs
	default:
		return false
	}
}

func TestLabelSelector(t *testing.T) {
	t.Run("when empty LabelSelector is built, it makes empty", func(t *testing.T) {
		testee := k8s.LabelSelector{}
		if testee.QueryString() != "" {
			t.Errorf(`not match: "%s" is not empty`, testee.QueryString())
		}
	})

	t.Run("when LabelSelector is not empty,", func(t *testing.T) {
		testee := k8s.LabelSelector{
			"foo":  FakeSelector("bar"),
			"fizz": FakeSelector("bazz"),
			"aaa":  FakeSelector("bbb"),
		}

		t.Run("its QueryString should be comma-separeated QueryStrings of selectors", func(t *testing.T) {
			actual := testee.QueryString()
			expected := []string{
				"foo:bar", "fizz:bazz", "aaa:bbb",
			}

			if !cmp.SliceContentEq(strings.Split(actual, ","), expected) {
				t.Error("not match: actual =", actual)
			}
		})
	})
}

func TestEqualityBasedSelector(t *testing.T) {
	for name, testcase := range map[string]struct {
		when string
		then string
	}{
		`when its value is not started with =, == not !=, it should mean "equality"`: {
			when: "value1", then: "label=value1",
		},
		`when its value is started with =, it should mean "equality"`: {
			when: "=value2", then: "label=value2",
		},
		`when its value is started with ==, it should mean "equality"`: {
			when: "==value3", then: "label=value3",
		},
		`when its value is started with !=, it should mean "inequality"`: {
			when: "!=value4", then: "label!=value4",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Run("as QueryString", func(t *testing.T) {
				actual := k8s.EqualityBased(testcase.when).QueryString("label")
				if actual != testcase.then {
					t.Errorf(
						"not match: (actual, expected) = (`%s`, `%s`)",
						actual, testcase.then,
					)
				}
			})
		})
	}

	for name, testcase := range map[string]struct {
		eq  []k8s.EqualityBased
		neq []k8s.EqualityBased
	}{
		"for equality operators": {
			eq: []k8s.EqualityBased{
				"value", "=value", "==value",
			},
			neq: []k8s.EqualityBased{
				"other",   // value is different
				"!=value", // operator is different
			},
		},
		"for inequality operators": {
			eq: []k8s.EqualityBased{
				"!=value",
			},
			neq: []k8s.EqualityBased{
				"!=other", // value is different
				"=value",  // operator is different
			},
		},
	} {
		t.Run("it is equal for other EqalityBased Selector as long as both of operator and value are same"+name, func(t *testing.T) {
			for _, a := range testcase.eq {
				for _, b := range testcase.eq {
					if !a.Equal(b) {
						t.Errorf("unexpected: %s != %s", a, b)
					}
				}
			}
			for _, a := range testcase.eq {
				for _, b := range testcase.neq {
					if a.Equal(b) {
						t.Errorf("unexpected: %s == %s", a, b)
					}
					if b.Equal(a) {
						t.Errorf("unexpected: %s == %s", a, b)
					}
				}
			}
		})
	}
}
