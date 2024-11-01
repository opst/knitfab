package garbage

import (
	"github.com/opst/knitfab/pkg/domain/garbage/db"
	"github.com/opst/knitfab/pkg/domain/garbage/k8s"
)

type Interface interface {
	Database() db.Interface
	K8s() k8s.Interface
}

type Garbage struct {
	db  db.Interface
	k8s k8s.Interface
}

func New(dbg db.Interface, k8s k8s.Interface) Interface {
	return &Garbage{db: dbg, k8s: k8s}
}

func (g *Garbage) Database() db.Interface {
	return g.db
}

func (g *Garbage) K8s() k8s.Interface {
	return g.k8s
}
