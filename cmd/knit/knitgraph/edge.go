package knitgraph

import (
	"fmt"
	"io"
)

type Edge struct {
	FromId NodeId
	ToId   NodeId
	Label  string
}

func (e Edge) ToDot(w io.Writer) error {
	label := ""
	if e.Label != "" {
		label = fmt.Sprintf(` [label="%s"]`, e.Label)
	}

	_, err := fmt.Fprintf(
		w,
		`	"%s" -> "%s"%s;
`,
		e.FromId, e.ToId, label,
	)
	return err
}
