package cluster

import (
	"strings"
)

type Label struct {
	Key   string
	Value string
}

// k8s Label SelectorElement like EqualityBased or SetBased
type SelectorElement interface {
	// convert to querystring expression for label
	QueryString(label string) string

	// return true if this is equal to other. otherwise false.
	//
	// this method SHOULD return false when other is not same struct for itself.
	Equal(other SelectorElement) bool
}

type LabelSelector map[string]SelectorElement

// convert to string value in form of query string.
func (ls LabelSelector) QueryString() string {
	if len(ls) == 0 {
		return ""
	}

	b := &strings.Builder{}
	for k, v := range ls {
		b.WriteString(v.QueryString(k))
		b.WriteRune(',')
	}
	s := b.String()
	return s[:len(s)-1] // trim rightmost comma
}

// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#equality-based-requirement
type EqualityBased string

var _ SelectorElement = EqualityBased("")

func NotEq(value string) EqualityBased {
	_, v := EqualityBased(value).destract()
	return EqualityBased("!=" + v)
}

func Eq(value string) EqualityBased {
	_, v := EqualityBased(value).destract()
	return EqualityBased("=" + v)
}

func (eqb EqualityBased) destract() (operator string, value string) {
	exp := string(eqb)
	if exp == "" {
		return "=", ""
	}

	switch exp[0] {
	case '=':
		offset := 1
		operator = "="
		if exp[1] == '=' {
			offset += 1
		}
		value = exp[offset:]
	case '!':
		offset := 1
		operator = "!="
		if eqb[1] == '=' {
			offset += 1
		} else {
			offset -= 1 // "!foo" does not mean "!=foo" .
			operator = "="
		}

		value = exp[offset:]
	default:
		operator = "="
		value = exp
	}

	return operator, value
}

func (eqb EqualityBased) QueryString(label string) string {
	op, v := eqb.destract()
	return label + op + v
}

func (eqb EqualityBased) Equal(other SelectorElement) bool {
	switch o := other.(type) {
	case EqualityBased:
		op, v := eqb.destract()
		oop, ov := o.destract()
		return op == oop && v == ov
	default:
		return false
	}
}

func LabelsToSelecor(ls map[string]string) LabelSelector {
	new := LabelSelector{}
	for k, v := range ls {
		new[k] = EqualityBased(v)
	}
	return new
}
