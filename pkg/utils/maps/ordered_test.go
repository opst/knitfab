package maps

import (
	"testing"

	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/tuple"
)

func TestOrderedMap(t *testing.T) {
	testee := NewOrderedMap(
		tuple.PairOf(1, "one"),
		tuple.PairOf(-2, "two"),
	)
	testee.Set(3, "three")

	{
		v, ok := testee.Get(1)
		if !ok {
			t.Errorf("Expected key 1 to be present")
		}
		if v != "one" {
			t.Errorf("Expected value to be one, got %s", v)
		}
	}
	{
		v, ok := testee.Get(-2)
		if !ok {
			t.Errorf("Expected key 2 to be present")
		}
		if v != "two" {
			t.Errorf("Expected value to be two, got %s", v)
		}
	}
	{
		v, ok := testee.Get(3)
		if !ok {
			t.Errorf("Expected key 3 to be present")
		}
		if v != "three" {
			t.Errorf("Expected value to be three, got %s", v)
		}
	}
	{
		_, ok := testee.Get(4)
		if ok {
			t.Errorf("Expected key 4 to be absent")
		}
	}

	{
		got := testee.Len()
		if want := 3; got != want {
			t.Errorf("Expected length to be %d, got %d", want, got)
		}
	}

	{
		keys := testee.Keys()
		if want := []int{1, -2, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
	}
	{
		values := testee.Values()
		if want := []string{"one", "two", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}
	{
		keys := []int{}
		values := []string{}

		for k, v := range testee.Iter() {
			keys = append(keys, k)
			values = append(values, v)
		}

		if want := []int{1, -2, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
		if want := []string{"one", "two", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}

	testee.Delete(-2)

	{
		got, ok := testee.Get(1)
		if !ok {
			t.Errorf("Expected key 1 to be present")
		}
		if got != "one" {
			t.Errorf("Expected value to be one, got %s", got)
		}
	}
	{
		_, ok := testee.Get(-2)
		if ok {
			t.Errorf("Expected key 2 to be absent")
		}
	}
	{
		got, ok := testee.Get(3)
		if !ok {
			t.Errorf("Expected key 3 to be present")
		}
		if got != "three" {
			t.Errorf("Expected value to be three, got %s", got)
		}
	}
	{
		_, ok := testee.Get(4)
		if ok {
			t.Errorf("Expected key 4 to be absent")
		}
	}
	{
		got := testee.Len()
		if want := 2; got != want {
			t.Errorf("Expected length to be %d, got %d", want, got)
		}
	}
	{
		keys := testee.Keys()
		if want := []int{1, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
	}

	{
		values := testee.Values()
		if want := []string{"one", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}
	{
		keys := []int{}
		values := []string{}

		for k, v := range testee.Iter() {
			keys = append(keys, k)
			values = append(values, v)
		}

		if want := []int{1, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
		if want := []string{"one", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}

	testee.Delete(2)  // Delete non-existent key
	testee.Delete(-1) // Delete non-existent key

	{
		got, ok := testee.Get(1)
		if !ok {
			t.Errorf("Expected key 1 to be present")
		}
		if got != "one" {
			t.Errorf("Expected value to be one, got %s", got)
		}
	}
	{
		_, ok := testee.Get(-2)
		if ok {
			t.Errorf("Expected key 2 to be absent")
		}
	}
	{
		got, ok := testee.Get(3)
		if !ok {
			t.Errorf("Expected key 3 to be present")
		}
		if got != "three" {
			t.Errorf("Expected value to be three, got %s", got)
		}
	}
	{
		_, ok := testee.Get(4)
		if ok {
			t.Errorf("Expected key 4 to be absent")
		}
	}
	{
		got := testee.Len()
		if want := 2; got != want {
			t.Errorf("Expected length to be %d, got %d", want, got)
		}
	}
	{
		keys := testee.Keys()
		if want := []int{1, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
	}

	{
		values := testee.Values()
		if want := []string{"one", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}
	{
		keys := []int{}
		values := []string{}

		for k, v := range testee.Iter() {
			keys = append(keys, k)
			values = append(values, v)
		}

		if want := []int{1, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
		if want := []string{"one", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}

	testee.Set(1, "ONE") // update value

	{
		got, ok := testee.Get(1)
		if !ok {
			t.Errorf("Expected key 1 to be present")
		}
		if got != "ONE" {
			t.Errorf("Expected value to be ONE, got %s", got)
		}
	}
	{
		got, ok := testee.Get(3)
		if !ok {
			t.Errorf("Expected key 3 to be present")
		}
		if got != "three" {
			t.Errorf("Expected value to be three, got %s", got)
		}
	}
	{
		got := testee.Len()
		if want := 2; got != want {
			t.Errorf("Expected length to be %d, got %d", want, got)
		}
	}
	{
		keys := testee.Keys()
		if want := []int{1, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
	}
	{
		values := testee.Values()
		if want := []string{"ONE", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}
	{
		keys := []int{}
		values := []string{}

		for k, v := range testee.Iter() {
			keys = append(keys, k)
			values = append(values, v)
		}

		if want := []int{1, 3}; !cmp.SliceEq(keys, want) {
			t.Errorf("Expected keys to be %v, got %v", want, keys)
		}
		if want := []string{"ONE", "three"}; !cmp.SliceEq(values, want) {
			t.Errorf("Expected values to be %v, got %v", want, values)
		}
	}

}
