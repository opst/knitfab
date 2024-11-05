package keychain

import (
	"github.com/opst/knitfab/pkg/domain/keychain/db"
	"github.com/opst/knitfab/pkg/domain/keychain/k8s"
)

type Interface interface {
	Database() db.KeychainInterface
	K8s() k8s.Interface
}

type impl struct {
	db db.KeychainInterface
	kc k8s.Interface
}

func New(db db.KeychainInterface, kc k8s.Interface) Interface {
	return &impl{db: db, kc: kc}
}

func (i *impl) Database() db.KeychainInterface {
	return i.db
}

func (i *impl) K8s() k8s.Interface {
	return i.kc
}
