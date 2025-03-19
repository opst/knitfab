package domain

type Nomination struct {
	// nominating plan
	PlanBody

	// mountpoint where the plan nominates data onto.
	MountPoint
}

func (n *Nomination) Equal(other *Nomination) bool {
	if n == nil || other == nil {
		return n == other
	}
	return n.PlanBody.Equal(&other.PlanBody) &&
		n.MountPoint.Equal(&other.MountPoint)
}
