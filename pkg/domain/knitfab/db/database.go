package db

import (
	kdata "github.com/opst/knitfab/pkg/domain/data/db"
	kgarbage "github.com/opst/knitfab/pkg/domain/garbage/db"
	kkeychain "github.com/opst/knitfab/pkg/domain/keychain/db"
	kplan "github.com/opst/knitfab/pkg/domain/plan/db"
	krun "github.com/opst/knitfab/pkg/domain/run/db"
	kschema "github.com/opst/knitfab/pkg/domain/schema/db"
)

type KnitDatabase interface {
	Data() kdata.DataInterface
	Run() krun.RunInterface
	Plan() kplan.PlanInterface
	Garbage() kgarbage.GarbageInterface
	Schema() kschema.SchemaInterface
	Keychain() kkeychain.KeychainInterface
	Close() error
}
