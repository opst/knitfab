package data

import (
	"github.com/opst/knitfab/pkg/domain/data/db"
	"github.com/opst/knitfab/pkg/domain/data/k8s"
)

type Interface interface {
	Database() db.DataInterface
	K8s() k8s.Interface
}

type impl struct {
	database db.DataInterface
	dataagt  k8s.Interface
}

func New(database db.DataInterface, dataagt k8s.Interface) Interface {
	return &impl{
		database: database,
		dataagt:  dataagt,
	}
}

func (i *impl) Database() db.DataInterface {
	return i.database
}

func (i *impl) K8s() k8s.Interface {
	return i.dataagt
}
