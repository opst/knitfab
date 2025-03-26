package plan

import "github.com/opst/knitfab/v2/pkg/domain/plan/db"

type Interface interface {
	Database() db.PlanInterface
}

type impl struct {
	db db.PlanInterface
}

func New(db db.PlanInterface) Interface {
	return &impl{db: db}
}

func (i *impl) Database() db.PlanInterface {
	return i.db
}
