package maps

func DerefOf[K comparable, V any](m map[K]*V) map[K]V {
	ret := map[K]V{}
	for k := range m {
		ret[k] = *m[k]
	}
	return ret
}

func RefOf[K comparable, V any](m map[K]V) map[K]*V {
	ret := map[K]*V{}
	for k := range m {
		v := m[k]
		ret[k] = &v
	}
	return ret
}

// Map is a generic interface for a map-like data structure.
type Map[K comparable, V any] interface {
	// Set sets the value for the key.
	//
	// If the key is already present, the value is overwritten.
	Set(k K, v V)

	// Get returns the value for the key and a boolean indicating if the key was present.
	Get(k K) (V, bool)

	// Keys returns the keys in the map.
	Keys() []K

	// Values returns the values in the map.
	Values() []V

	// Keys returns the keys in the map.
	Delete(k K)

	// Len returns the number of keys in the map.
	Len() int

	// Iter returns a function that iterates over the map.
	Iter() func(yield func(k K, v V) bool)

	// ToMap returns a map[K]V.
	ToMap() map[K]V
}
