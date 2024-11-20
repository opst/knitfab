package schema

import "github.com/opst/knitfab/pkg/domain/schema/db"

type Interface interface {
	Database() db.SchemaInterface
}

type impl struct {
	db db.SchemaInterface
}

func New(db db.SchemaInterface) Interface {
	return &impl{db: db}
}

func (i *impl) Database() db.SchemaInterface {
	return i.db
}
