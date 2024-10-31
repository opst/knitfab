package run

import (
	"github.com/opst/knitfab/pkg/domain/run/db"
	"github.com/opst/knitfab/pkg/domain/run/k8s"
)

type Interface interface {
	Database() db.RunInterface
	K8s() k8s.Interface
}

type impl struct {
	db     db.RunInterface
	worker k8s.Interface
}

func New(db db.RunInterface, worker k8s.Interface) Interface {
	return &impl{db: db, worker: worker}
}

func (i *impl) Database() db.RunInterface {
	return i.db
}

func (i *impl) K8s() k8s.Interface {
	return i.worker
}
