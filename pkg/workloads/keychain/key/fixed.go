package key

type fixedKeyPolicy struct {
	k Key
}

func Fixed(k Key) KeyPolicy {
	return &fixedKeyPolicy{k: k}
}

func (fk *fixedKeyPolicy) Issue() (Key, error) {
	return fk.k, nil
}

type failingKeyPolicy struct {
	err error
}

func Failing(err error) KeyPolicy {
	return &failingKeyPolicy{err: err}
}

func (fk *failingKeyPolicy) Issue() (Key, error) {
	return nil, fk.err
}
