package maps

import "github.com/opst/knitfab/pkg/utils/tuple"

type orderedMap[K comparable, V any] struct {
	keys []K
	m    map[K]V
}

// NewOrderedMap creates a new ordered map with the given initial key-value pairs.
//
// The keys will be ordered in the order they were added.
func NewOrderedMap[K comparable, V any](initial ...tuple.Pair[K, V]) Map[K, V] {
	m := &orderedMap[K, V]{
		keys: []K{},
		m:    map[K]V{},
	}

	for _, pair := range initial {
		m.Set(pair.First, pair.Second)
	}

	return m
}

func (m *orderedMap[K, V]) Set(k K, v V) {
	if _, ok := m.m[k]; !ok {
		m.keys = append(m.keys, k)
	}
	m.m[k] = v
}

func (m *orderedMap[K, V]) Get(k K) (V, bool) {
	v, ok := m.m[k]
	return v, ok
}

func (m *orderedMap[K, V]) Keys() []K {
	return m.keys
}

func (m *orderedMap[K, V]) Values() []V {
	values := make([]V, len(m.keys))
	for i, k := range m.keys {
		values[i] = m.m[k]
	}
	return values
}

func (m *orderedMap[K, V]) Delete(k K) {
	delete(m.m, k)
	for i, key := range m.keys {
		if key == k {
			m.keys = append(m.keys[:i], m.keys[i+1:]...)
			break
		}
	}
}

func (m *orderedMap[K, V]) Len() int {
	return len(m.keys)
}

func (m *orderedMap[K, V]) Iter() func(yield func(k K, v V) bool) {
	return func(yield func(k K, v V) bool) {
		for _, k := range m.keys {
			v := m.m[k]
			if !yield(k, v) {
				break
			}
		}
	}
}

func (m *orderedMap[K, V]) ToMap() map[K]V {
	// Return a copy of the map to prevent modification of the internal map.
	ret := map[K]V{}
	for k, v := range m.m {
		ret[k] = v
	}
	return ret
}
