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
