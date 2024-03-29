package db

type KnitDatabase interface {
	Data() DataInterface
	Runs() RunInterface
	//	MountPoint() MountPointInterface
	Plan() PlanInterface
	Garbage() GarbageInterface
	Close() error
}
