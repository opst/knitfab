package knitgraph

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/opst/knitfab-api-types/data"
	"github.com/opst/knitfab-api-types/misc/rfctime"
	"github.com/opst/knitfab-api-types/runs"
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
	"github.com/opst/knitfab/pkg/utils/cmp"
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

type RunNode struct {
	NodeId NodeId
	runs.Detail
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

	_, err := fmt.Fprintf(
		w,
		`	"%s"[
		shape=none
		color=orange
		label=<
			<TABLE CELLSPACING="0">
				<TR><TD BGCOLOR="orange"><FONT COLOR="#FFFFFF"><B>Run</B></FONT></TD><TD>%s</TD><TD>id: %s</TD></TR>
				<TR><TD COLSPAN="3"><FONT POINT-SIZE="8">last updated: %s</FONT></TD></TR>
				<TR><TD COLSPAN="3">%s</TD></TR>
			</TABLE>
		>
	];
`,
		r.NodeId,
		status,
		html.EscapeString(r.RunId),
		html.EscapeString(r.UpdatedAt.Time().Local().Format(rfctime.RFC3339DateTimeFormat)),
		html.EscapeString(title),
	)
	return err
}

type DataNode struct {
	NodeId NodeId
	data.Detail
}

func (d *DataNode) Equal(o *DataNode) bool {
	return d.KnitId == o.KnitId &&
		cmp.SliceContentEqWith(d.Tags, o.Tags, tags.Tag.Equal)
}

func (d *DataNode) ToDot(w io.Writer, isArgKnitId bool) error {
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
	bgColor := map[bool]string{true: "#d4ecc6", false: "#FFFFFF"}[isArgKnitId]
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
		bgColor,
		html.EscapeString(knitId),
		subheader,
		strings.Join(userTags, "<BR/>"),
	)
	return err

}
