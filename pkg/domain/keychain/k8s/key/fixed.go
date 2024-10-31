package key

type fixedKeyPolicy struct {
	k Key
}

// Fixed returns a KeyPolicy that always returns the same key.
func Fixed(k Key) KeyPolicy {
	return &fixedKeyPolicy{k: k}
}

func (fk *fixedKeyPolicy) Issue() (Key, error) {
	return fk.k, nil
}

type failingKeyPolicy struct {
	err error
}

// Failing returns a KeyPolicy that always fails with the given error.
//
// This is useful for testing.
func Failing(err error) KeyPolicy {
	return &failingKeyPolicy{err: err}
}

func (fk *failingKeyPolicy) Issue() (Key, error) {
	return nil, fk.err
}
