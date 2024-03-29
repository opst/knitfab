package db

type Nomination struct {
	// nominating plan
	PlanBody

	// mountpoint where the plan nominates data onto.
	MountPoint
}
