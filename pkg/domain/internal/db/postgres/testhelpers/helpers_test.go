package testhelpers_test

import (
	"strings"
	"testing"

	"github.com/opst/knitfab/pkg/domain/internal/db/postgres/testhelpers"
)

func TestPaddingX(t *testing.T) {

	//	func PaddingX[S ~string](x int, str S, padstr S) S

	type myString string
	type when struct {
		x      int
		str    myString
		padstr rune
	}

	for name, testcase := range map[string]struct {
		when
		then myString
	}{
		"When 1byte padding character is specified, it should fill padding characters until the specified string length is reached.": {
			when: when{
				x:      36,
				str:    "test-1",
				padstr: 'a',
			},
			then: "test-1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		"When multiple byte padding character is specified, it should fill padding characters until the specified string length is reached.": {
			when: when{
				x:      36,
				str:    "テスト-1",
				padstr: 'あ',
			},
			then: "テスト-1あああああああああああああああああああああああああああああああ",
		},
		"When the target string is longer than the specified string length, it should truncates the target string to the specified string length.": {
			when: when{
				x:      36,
				str:    "テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1",
				padstr: 'あ',
			},
			then: "テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1テ",
		},
		"When the specified string length is invalid (less than or equal to zero), it should return an empty string.": {
			when: when{
				x:      0,
				str:    "テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1テスト-1",
				padstr: 'あ',
			},
			then: "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			when, then := testcase.when, testcase.then
			actual := testhelpers.PaddingX(when.x, when.str, when.padstr)

			if strings.Compare(string(actual), string(then)) != 0 {
				t.Errorf(
					"Padded string does not match. (actual, expected) = \n(%s, \n%s)",
					actual, then,
				)
			}
		})
	}
}
