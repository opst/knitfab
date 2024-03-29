package utils_test

import (
	"testing"

	"github.com/opst/knitfab/pkg/utils"
)

func TestIfNotNil(t *testing.T) {
	refStr := func(v string) *string {
		return &v
	}
	refInt := func(v int) *int {
		return &v
	}
	mapper := func(v *string) *int {
		l := len(*v)
		return &l
	}
	type when struct {
		v      *string
		mapper func(*string) *int
	}
	for name, testcase := range map[string]struct {
		when
		then *int
	}{
		"when it is passed non-nil value, it proxies mapper": {
			when{
				v:      refStr("value"),
				mapper: mapper,
			},
			refInt(5),
		},
		"when it is passed nil value, it returns nil": {
			when{
				v:      nil,
				mapper: mapper,
			},
			nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			when, then := testcase.when, testcase.then
			actual := utils.IfNotNil(when.v, when.mapper)

			if then == nil {
				if actual != nil {
					t.Errorf("not match:\n- actual: %v\n- expected: <nil>", *actual)
				}
			} else if actual == nil {
				t.Errorf("not match:\n- actual: <nil>\n- expected: %v", *then)
			} else {
				if *actual != *then {
					t.Errorf("not match:\n- actual: %v\n- expected: %v", *actual, *then)
				}
			}
		})
	}
}

func TestDefault(t *testing.T) {
	ref := func(v string) *string {
		return &v
	}
	type when struct {
		v *string
		d string
	}
	for name, testcase := range map[string]struct {
		when
		then string
	}{
		"when it is passed a non-nil, it returns the value of it passed": {
			when{
				v: ref("value"),
				d: "default",
			},
			"value",
		},
		"when it is passed a nil, it returns the default value": {
			when{
				v: nil,
				d: "default",
			},
			"default",
		},
	} {
		t.Run(name, func(t *testing.T) {
			when := testcase.when
			actual := utils.Default(when.v, when.d)

			if actual != testcase.then {
				t.Errorf("not match:\n- actual   : %v\n- expected : %v", actual, testcase.then)
			}
		})
	}
}
