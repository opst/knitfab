package garbage

import (
	"github.com/opst/knitfab/pkg/domain/garbage/db"
)

type Interface interface {
	Database() db.GarbageInterface
}

type Garbage struct {
	db db.GarbageInterface
}

func New(dbg db.GarbageInterface) Interface {
	return &Garbage{db: dbg}
}

func (g *Garbage) Database() db.GarbageInterface {
	return g.db
}
