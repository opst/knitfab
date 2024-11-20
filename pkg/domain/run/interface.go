package run

import (
	"github.com/opst/knitfab/pkg/domain/run/db"
	"github.com/opst/knitfab/pkg/domain/run/k8s"
)

type Interface interface {
	Database() db.Interface
	K8s() k8s.Interface
}

type impl struct {
	db     db.Interface
	worker k8s.Interface
}

func New(db db.Interface, worker k8s.Interface) Interface {
	return &impl{db: db, worker: worker}
}

func (i *impl) Database() db.Interface {
	return i.db
}

func (i *impl) K8s() k8s.Interface {
	return i.worker
}
