package strings

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// `TrimPrefixAll` returns string `s` without provided `prefix`es.
// If `prefix`es are repeated, all of them are removed.
//
// example:
//      TrimPrefixAll("aaabbbccc", "aaab")  // -> "bbccc" : prefix is trimmed
//      TrimPrefixAll("aaabbbccc", "a")     // -> "bbbccc" : prefix is trimmed repeatedly
//      TrimPrefixAll("aaabbccc", "x")      // -> "aaabbbccc" : if no prefix is found, `s` is returned unchanged
//
func TrimPrefixAll(s, prefix string) string {
	lp := len(prefix)

	for strings.HasPrefix(s, prefix) {
		s = s[lp:]
	}
	return s
}

// supply suffix if text has not.
//
// args:
//     - text: target text
//     - suffix: suffix
// return:
//     text same as input when that has suffix.
//     otherwise, text + suffix.
func SuppySuffix(text, suffix string) string {
	if strings.HasSuffix(text, suffix) {
		return text
	}
	return text + suffix
}

// return random Hex string (/[0-9a-f]*/)
func RandomHex(l uint) (string, error) {
	if l == 0 {
		return "", nil
	}

	// encoding from []byte to hex string is doubling its length.
	// in case of odd `l`, add extra 1 not to be short.
	buffer := make([]byte, l/2+1)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer)[:l], nil
}

// like strings.Split(s, sep), but return empty slice when s == ""
func SplitIfNotEmpty(s string, sep string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, sep)
}

func SprintMany(template string, choices ...[]any) []string {
	numchoice := len(choices)
	if numchoice == 0 {
		return []string{}
	}
	size := 1
	for _, c := range choices {
		if len(c) == 0 {
			return []string{}
		}
		size *= len(c)
	}

	var pattern func([][]any) [][]any

	pattern = func(choices [][]any) [][]any {
		rest, tail := choices[:len(choices)-1], choices[len(choices)-1]

		if len(rest) == 0 {
			ret := make([][]any, 0, len(tail))
			for _, t := range tail {
				seed := make([]any, 1, numchoice)
				seed[0] = t
				ret = append(ret, seed)
			}
			return ret
		}

		restSize := 1
		for _, r := range rest {
			restSize *= len(r)
		}
		ret := make([][]any, 0, restSize)
		for _, t := range tail {
			for _, b := range pattern(rest) {
				ret = append(ret, append(b, t))
			}
		}
		return ret
	}

	ret := make([]string, 0, size)
	for _, p := range pattern(choices) {
		ret = append(ret, fmt.Sprintf(template, p...))
	}
	return ret
}
