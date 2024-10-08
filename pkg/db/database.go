package db

type KnitDatabase interface {
	Data() DataInterface
	Runs() RunInterface
	Plan() PlanInterface
	Garbage() GarbageInterface
	Schema() SchemaInterface
	Keychain() KeychainInterface
	Close() error
}
