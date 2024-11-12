package knitgraph

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/plans"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/cmp"
	"github.com/opst/knitfab/pkg/utils/maps"
)

type RootNode struct {
	NodeId NodeId
}

func (r *RootNode) Equal(o *RootNode) bool {
	return r.NodeId == o.NodeId
}

func (r *RootNode) ToDot(w io.Writer) error {
	_, err := fmt.Fprintf(
		w,
		`	"%s"[shape=Mdiamond];
`,
		r.NodeId,
	)
	return err
}

type InputNode struct {
	NodeId NodeId
	plans.Input
}

func (in *InputNode) Equal(o *InputNode) bool {
	return in.NodeId == o.NodeId && in.Equal(o)
}

func (in *InputNode) ToDot(w io.Writer) error {
	_, err := fmt.Fprintf(
		w,
		`		"%s"[shape=point, color="#1c9930"];
`,
		in.NodeId,
	)

	return err
}

type OutputNode struct {
	NodeId NodeId
	plans.Output
}

func (o *OutputNode) Equal(oo *OutputNode) bool {
	return o.NodeId == oo.NodeId && o.Equal(oo)
}

func (o *OutputNode) ToDot(w io.Writer) error {
	_, err := fmt.Fprintf(
		w,
		`		"%s"[shape=point, color="#1c9930"];
`,
		o.NodeId,
	)

	return err
}

type PlanLogNode struct {
	NodeId NodeId
	plans.Log
}

func (l *PlanLogNode) Equal(o *PlanLogNode) bool {
	if (o == nil) != (l == nil) {
		return false
	}
	if l == nil {
		return true
	}
	return l.NodeId == o.NodeId && l.Equal(o)
}

func (l *PlanLogNode) ToDot(w io.Writer) error {
	_, err := fmt.Fprintf(
		w,
		`		"%s"[shape=point, color="#1c9930"];
`,
		l.NodeId,
	)

	return err
}

type PlanNode struct {
	NodeId NodeId
	plans.Detail

	InputNodes  maps.Map[string, InputNode]
	OutputNodes maps.Map[string, OutputNode]
	LogNode     *PlanLogNode

	Emphasize bool
}

func (p *PlanNode) Equal(o *PlanNode) bool {
	if (o == nil) != (p == nil) {
		return false
	}
	if p == nil {
		return true
	}
	return p.NodeId == o.NodeId &&
		p.Equal(o) &&
		cmp.MapEqWith(p.InputNodes.ToMap(), o.InputNodes.ToMap(), func(a, b InputNode) bool { return a.Equal(&b) }) &&
		cmp.MapEqWith(p.OutputNodes.ToMap(), o.OutputNodes.ToMap(), func(a, b OutputNode) bool { return a.Equal(&b) }) &&
		p.LogNode.Equal(o.LogNode) &&
		p.Emphasize == o.Emphasize
}

func (p *PlanNode) ToDot(w io.Writer) error {
	title := ""
	if p.Image != nil {
		title = "image = " + p.Image.String()
	} else if p.Name != "" {
		title = p.Name
	}

	annotations := []string{}
	for _, a := range p.Annotations {
		annotations = append(annotations, fmt.Sprintf(`<B>%s</B>=%s`, html.EscapeString(a.Key), html.EscapeString(a.Value)))
	}
	{
		_, err := fmt.Fprintf(w, `	subgraph {
`)
		if err != nil {
			return err
		}
	}

	for _, in := range p.InputNodes.Iter() {
		if err := in.ToDot(w); err != nil {
			return err
		}

		_, err := fmt.Fprintf(
			w,
			`		"%s" -> "%s" [label="%s"];
`,
			in.NodeId, p.NodeId, in.Input.Mountpoint.Path,
		)
		if err != nil {
			return err
		}
	}

	{
		idBgColor := "#FFFFFF"
		if p.Emphasize {
			idBgColor = "#EDD9B4"
		}
		_, err := fmt.Fprintf(
			w,
			`		"%s"[
			shape=none
			color="#DAA520"
			label=<
				<TABLE CELLSPACING="0">
					<TR><TD BGCOLOR="#DAA520"><FONT COLOR="#FFFFFF"><B>Plan</B></FONT></TD><TD BGCOLOR="%s">id: %s</TD></TR>
					<TR><TD COLSPAN="2">%s</TD></TR>
					<TR><TD COLSPAN="2">%s</TD></TR>
				</TABLE>
			>
		];
`,
			p.NodeId,
			idBgColor, p.PlanId,
			html.EscapeString(title),
			strings.Join(annotations, "<BR/>"),
		)
		if err != nil {
			return err
		}
	}

	for _, out := range p.OutputNodes.Iter() {
		if err := out.ToDot(w); err != nil {
			return err
		}

		_, err := fmt.Fprintf(
			w,
			`		"%s" -> "%s" [label="%s"];
`,
			p.NodeId, out.NodeId, out.Output.Mountpoint.Path,
		)
		if err != nil {
			return err
		}
	}

	if p.LogNode != nil {
		if err := p.LogNode.ToDot(w); err != nil {
			return err
		}

		_, err := fmt.Fprintf(
			w,
			`		"%s" -> "%s" [label="(log)"];
`,
			p.NodeId, p.LogNode.NodeId,
		)
		if err != nil {
			return err
		}
	}

	{
		_, err := fmt.Fprintf(
			w,
			`	}
`,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

type RunNode struct {
	NodeId NodeId
	runs.Detail

	Emphasize bool
}

func (r *RunNode) Equal(o *RunNode) bool {
	if (o == nil) != (r == nil) {
		return false
	}
	if r == nil {
		return true
	}
	return r.NodeId == o.NodeId && r.Equal(o) && r.Emphasize == o.Emphasize
}

func (r *RunNode) ToDot(w io.Writer) error {
	title := ""
	if r.Plan.Image != nil {
		title = "image = " + r.Plan.Image.String()
	} else if r.Plan.Name != "" {
		title = r.Plan.Name
	}

	status := html.EscapeString(r.Status)
	switch domain.KnitRunStatus(r.Status) {
	case domain.Deactivated:
		status = fmt.Sprintf(`<FONT COLOR="gray"><B>%s</B></FONT>`, status)
	case domain.Completing, domain.Done:
		status = fmt.Sprintf(`<FONT COLOR="#007700"><B>%s</B></FONT>`, status)
	case domain.Aborting, domain.Failed:
		status = fmt.Sprintf(`<FONT COLOR="red"><B>%s</B></FONT>`, status)
	}

	idBgColor := "#FFFFFF"
	if r.Emphasize {
		idBgColor = "#FFD580"
	}

	_, err := fmt.Fprintf(
		w,
		`	"%s"[
		shape=none
		color="#FFA500"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#FFA500"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD>%s</TD><TD BGCOLOR="%s">id: %s</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">%s</TD></TR>
			</TABLE>
		>
	];
`,
		r.NodeId,
		status, idBgColor, html.EscapeString(r.RunId),
		html.EscapeString(r.UpdatedAt.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
		html.EscapeString(title),
	)
	return err
}

type DataNode struct {
	NodeId NodeId
	data.Detail
	Emphasize bool
}

func (d *DataNode) Equal(o *DataNode) bool {
	return d.KnitId == o.KnitId &&
		d.Detail.Equal(o.Detail) &&
		d.Emphasize == o.Emphasize
}

func (d *DataNode) ToDot(w io.Writer) error {
	knitId := d.KnitId

	systemtag := []string{}
	userTags := []string{}
	for _, tag := range d.Tags {
		switch tag.Key {
		case domain.KeyKnitTimestamp:
			tsp, err := rfctime.ParseRFC3339DateTime(tag.Value)
			if err != nil {
				return err
			}
			systemtag = append(
				systemtag,
				html.EscapeString(tsp.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
			)
			continue
		case domain.KeyKnitTransient:
			systemtag = append(systemtag, html.EscapeString(tag.String()))
			continue
		case tags.KeyKnitId:
			continue
		}

		userTags = append(
			userTags,
			fmt.Sprintf(`<B>%s</B>:%s`, html.EscapeString(tag.Key), html.EscapeString(tag.Value)),
		)
	}
	subheader := ""
	if len(systemtag) != 0 {
		subheader = fmt.Sprintf(
			`<TR><TD COLSPAN="2"><FONT POINT-SIZE="8">%s</FONT></TD></TR>`,
			strings.Join(systemtag, " | "),
		)
	}

	//The background color of the data node that is the argument gets highlighted from the others.
	idBgColor := "#FFFFFF"
	if d.Emphasize {
		idBgColor = "#d4ecc6"
	}
	_, err := fmt.Fprintf(
		w,
		`	"%s"[
		shape=none
		color="#1c9930"
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="#1c9930"><FONT COLOR="#FFFFFF"><B>Data</B></FONT></TD><TD BGCOLOR="%s">knit#id: %s</TD></TR>
				%s
				<TR><TD COLSPAN="2">%s</TD></TR>
			</TABLE>
		>
	];
`,
		d.NodeId,
		idBgColor,
		html.EscapeString(knitId),
		subheader,
		strings.Join(userTags, "<BR/>"),
	)
	return err

}
