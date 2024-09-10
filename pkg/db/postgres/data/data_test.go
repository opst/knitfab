package data_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	kpgnommock "github.com/opst/knitfab/pkg/db/postgres/nominator/mock"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	"github.com/opst/knitfab/pkg/db/postgres/pool/proxy"
	testenv "github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/scanner"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
	"github.com/opst/knitfab/pkg/db/postgres/tables/matcher"
	. "github.com/opst/knitfab/pkg/db/postgres/testhelpers"
	"github.com/opst/knitfab/pkg/utils"
	"github.com/opst/knitfab/pkg/utils/rfctime"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestData_Find(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	type when struct {
		tags         []kdb.Tag
		updatedSince *time.Time
		updatedUntil *time.Time
	}
	type then struct {
		knitId []string
	}

	plan := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: Padding36("plan"), Active: true, Hash: Padding64("#plan")},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: Padding36("plan"), Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{OutputId: 1010, PlanId: Padding36("plan"), Path: "/out"}: {},
		},
	}

	runTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		"2022-10-11T12:13:14.567+09:00",
	)).OrFatal(t).Time()

	oldTime := "2022-11-12T13:14:15.678+09:00"
	oldTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		oldTime,
	)).OrFatal(t).Time()

	newTimestamp := try.To(rfctime.ParseRFC3339DateTime(
		"2022-11-13T14:15:16.678+09:00",
	)).OrFatal(t).Time()

	dummyUpdatedSinceA := try.To(rfctime.ParseRFC3339DateTime(oldTime)).OrFatal(t).Time().Add(time.Hour + time.Minute)

	dummyUpdatedSinceB := try.To(rfctime.ParseRFC3339DateTime(oldTime)).OrFatal(t).Time().Add(-time.Hour)
	dummyUpdatedUntilB := try.To(rfctime.ParseRFC3339DateTime(oldTime)).OrFatal(t).Time().Add(time.Hour)

	tagsetAll := []kdb.Tag{
		{Key: "tag-a", Value: "a-value"},
		{Key: "tag-b", Value: "b-value"},
		{Key: "tag-c", Value: "c-value"},
	}
	tagsetAandB := tagsetAll[:2]
	tagsetBandC := tagsetAll[1:]
	tagsetAandC := []kdb.Tag{tagsetAll[0], tagsetAll[2]}

	t.Run("(matrix test)", func(t *testing.T) {
		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		if err := plan.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}

		// generating data and its upstream
		for tagcode, tag := range map[string][]kdb.Tag{
			"a-and-b": tagsetAandB,
			"b-and-c": tagsetBandC,
			// a-and-c: no such data.
			"no-tags": {},
		} {
			for timecode, timestamp := range map[string]*time.Time{
				"old":     &oldTimestamp,
				"new":     &newTimestamp,
				"no-time": nil,
			} {
				for _, status := range []kdb.KnitRunStatus{
					// knit#transient: processing
					kdb.Deactivated, kdb.Waiting, kdb.Ready, kdb.Starting, kdb.Running, kdb.Aborting, kdb.Completing,

					// no knit#transient
					kdb.Done,

					// knit#transient: failed
					kdb.Failed, kdb.Invalidated,
				} {
					runid := Padding36(fmt.Sprintf(
						"run_%s_%s_%s", tagcode, timecode, status,
					))
					knitid := Padding36(fmt.Sprintf(
						"knit-%s-%s-%s", tagcode, timecode, status,
					))
					step := tables.Step{
						Run: tables.Run{
							RunId: runid, Status: status, PlanId: Padding36("plan"),
							UpdatedAt: runTimestamp,
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: knitid, VolumeRef: Padding64("#" + knitid),
								RunId: runid, OutputId: 1010, PlanId: Padding36("plan"),
							}: {
								UserTag: tag, Timestamp: timestamp,
							},
						},
					}
					if err := step.Apply(ctx, pool); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		for name, testcase := range map[string]struct {
			when
			then
		}{
			// system tags
			`when querying by "knit#transient: failed", it returns data come from Failed run`: {
				when{
					tags: []kdb.Tag{
						try.To(kdb.NewTag(kdb.KeyKnitTransient, kdb.ValueKnitTransientFailed)).OrFatal(t),
					},
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),

						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),

						Padding36("knit-no-tags-old-failed"),
						Padding36("knit-no-tags-old-invalidated"),

						Padding36("knit-a-and-b-new-failed"),
						Padding36("knit-a-and-b-new-invalidated"),

						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),

						Padding36("knit-no-tags-new-failed"),
						Padding36("knit-no-tags-new-invalidated"),

						Padding36("knit-a-and-b-no-time-failed"),
						Padding36("knit-a-and-b-no-time-invalidated"),

						Padding36("knit-b-and-c-no-time-failed"),
						Padding36("knit-b-and-c-no-time-invalidated"),

						Padding36("knit-no-tags-no-time-failed"),
						Padding36("knit-no-tags-no-time-invalidated"),
					},
				},
			},
			`when querying by "knit#transient: processing", it returns data come from unterminated run`: {
				when{
					tags: []kdb.Tag{
						try.To(kdb.NewTag(kdb.KeyKnitTransient, kdb.ValueKnitTransientProcessing)).OrFatal(t),
					},
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),

						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),

						Padding36("knit-no-tags-old-aborting"),
						Padding36("knit-no-tags-old-completing"),
						Padding36("knit-no-tags-old-deactivated"),
						Padding36("knit-no-tags-old-ready"),
						Padding36("knit-no-tags-old-running"),
						Padding36("knit-no-tags-old-starting"),
						Padding36("knit-no-tags-old-waiting"),

						Padding36("knit-a-and-b-new-aborting"),
						Padding36("knit-a-and-b-new-completing"),
						Padding36("knit-a-and-b-new-deactivated"),
						Padding36("knit-a-and-b-new-ready"),
						Padding36("knit-a-and-b-new-running"),
						Padding36("knit-a-and-b-new-starting"),
						Padding36("knit-a-and-b-new-waiting"),

						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),

						Padding36("knit-no-tags-new-aborting"),
						Padding36("knit-no-tags-new-completing"),
						Padding36("knit-no-tags-new-deactivated"),
						Padding36("knit-no-tags-new-ready"),
						Padding36("knit-no-tags-new-running"),
						Padding36("knit-no-tags-new-starting"),
						Padding36("knit-no-tags-new-waiting"),

						Padding36("knit-a-and-b-no-time-aborting"),
						Padding36("knit-a-and-b-no-time-completing"),
						Padding36("knit-a-and-b-no-time-deactivated"),
						Padding36("knit-a-and-b-no-time-ready"),
						Padding36("knit-a-and-b-no-time-running"),
						Padding36("knit-a-and-b-no-time-starting"),
						Padding36("knit-a-and-b-no-time-waiting"),

						Padding36("knit-b-and-c-no-time-aborting"),
						Padding36("knit-b-and-c-no-time-completing"),
						Padding36("knit-b-and-c-no-time-deactivated"),
						Padding36("knit-b-and-c-no-time-ready"),
						Padding36("knit-b-and-c-no-time-running"),
						Padding36("knit-b-and-c-no-time-starting"),
						Padding36("knit-b-and-c-no-time-waiting"),

						Padding36("knit-no-tags-no-time-aborting"),
						Padding36("knit-no-tags-no-time-completing"),
						Padding36("knit-no-tags-no-time-deactivated"),
						Padding36("knit-no-tags-no-time-ready"),
						Padding36("knit-no-tags-no-time-running"),
						Padding36("knit-no-tags-no-time-starting"),
						Padding36("knit-no-tags-no-time-waiting"),
					},
				},
			},
			`when querying by "knit#timestamp: ...", it returns data having timestamp at same time`: {
				when{
					tags: []kdb.Tag{
						kdb.NewTimestampTag(oldTimestamp),
					},
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-done"),
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),

						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-done"),
						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),

						Padding36("knit-no-tags-old-aborting"),
						Padding36("knit-no-tags-old-completing"),
						Padding36("knit-no-tags-old-deactivated"),
						Padding36("knit-no-tags-old-done"),
						Padding36("knit-no-tags-old-failed"),
						Padding36("knit-no-tags-old-invalidated"),
						Padding36("knit-no-tags-old-ready"),
						Padding36("knit-no-tags-old-running"),
						Padding36("knit-no-tags-old-starting"),
						Padding36("knit-no-tags-old-waiting"),
					},
				},
			},
			`when querying by "knit#timestamp: ..." but there are no such data, it returns nothing`: {
				when{
					tags: []kdb.Tag{
						kdb.NewTimestampTag(try.To(rfctime.ParseRFC3339DateTime(
							"3000-10-11T10:11:12.567+00:00",
						)).OrFatal(t).Time()),
					},
				},
				then{knitId: []string{}}, // empty!
			},
			`when querying by different "knit#timestamp: ..."s, it returns nothing`: {
				when{
					tags: []kdb.Tag{
						kdb.NewTimestampTag(newTimestamp),
						kdb.NewTimestampTag(oldTimestamp),
					},
				},
				then{knitId: []string{}}, // empty!
			},
			`when querying by "knit#id: ...", it returns the data`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitId, Value: Padding36("knit-a-and-b-new-done")},
					},
				},
				then{knitId: utils.Map([]string{
					"knit-a-and-b-new-done",
				}, Padding36[string])},
			},
			`when querying by "knit#id: ..." but no such data, it returns nothing`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitId, Value: Padding36("there-are-not-such-one")},
					},
				},
				then{knitId: []string{}},
			},
			`when querying by different "knit#id: ...", it returns nothing`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitId, Value: Padding36("knit-a-and-b-new-done")},
						{Key: kdb.KeyKnitId, Value: Padding36("knit-a-and-b-old-done")},
					},
				},
				then{knitId: []string{}},
			},

			// user tags
			`when querying by single user tag, it returns data which have the tag`: {
				when{
					tags: tagsetAll[1:2], // take only b
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-done"),
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),

						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-done"),
						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),

						Padding36("knit-a-and-b-new-aborting"),
						Padding36("knit-a-and-b-new-completing"),
						Padding36("knit-a-and-b-new-deactivated"),
						Padding36("knit-a-and-b-new-done"),
						Padding36("knit-a-and-b-new-failed"),
						Padding36("knit-a-and-b-new-invalidated"),
						Padding36("knit-a-and-b-new-ready"),
						Padding36("knit-a-and-b-new-running"),
						Padding36("knit-a-and-b-new-starting"),
						Padding36("knit-a-and-b-new-waiting"),

						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-done"),
						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),

						Padding36("knit-a-and-b-no-time-aborting"),
						Padding36("knit-a-and-b-no-time-completing"),
						Padding36("knit-a-and-b-no-time-deactivated"),
						Padding36("knit-a-and-b-no-time-done"),
						Padding36("knit-a-and-b-no-time-failed"),
						Padding36("knit-a-and-b-no-time-invalidated"),
						Padding36("knit-a-and-b-no-time-ready"),
						Padding36("knit-a-and-b-no-time-running"),
						Padding36("knit-a-and-b-no-time-starting"),
						Padding36("knit-a-and-b-no-time-waiting"),

						Padding36("knit-b-and-c-no-time-aborting"),
						Padding36("knit-b-and-c-no-time-completing"),
						Padding36("knit-b-and-c-no-time-deactivated"),
						Padding36("knit-b-and-c-no-time-done"),
						Padding36("knit-b-and-c-no-time-failed"),
						Padding36("knit-b-and-c-no-time-invalidated"),
						Padding36("knit-b-and-c-no-time-ready"),
						Padding36("knit-b-and-c-no-time-running"),
						Padding36("knit-b-and-c-no-time-starting"),
						Padding36("knit-b-and-c-no-time-waiting"),
					},
				},
			},
			`when querying by user tags, it returns data which have all of the tags`: {
				when{
					tags: tagsetBandC,
				},
				then{
					knitId: []string{
						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-done"),
						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),

						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-done"),
						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),

						Padding36("knit-b-and-c-no-time-aborting"),
						Padding36("knit-b-and-c-no-time-completing"),
						Padding36("knit-b-and-c-no-time-deactivated"),
						Padding36("knit-b-and-c-no-time-done"),
						Padding36("knit-b-and-c-no-time-failed"),
						Padding36("knit-b-and-c-no-time-invalidated"),
						Padding36("knit-b-and-c-no-time-ready"),
						Padding36("knit-b-and-c-no-time-running"),
						Padding36("knit-b-and-c-no-time-starting"),
						Padding36("knit-b-and-c-no-time-waiting"),
					},
				},
			},
			`when querying by user tags but no data have all of them, it returns nothing`: {
				when{
					tags: tagsetAandC, // no such data.
				},
				then{knitId: []string{}}, // empty!
			},
			`when querying by unknown tags, it returns nothing`: {
				when{
					tags: []kdb.Tag{
						{Key: "unknown", Value: "tag"},
					},
				},
				then{knitId: []string{}}, // empty!
			},

			// since and until
			`when querying by "since", it returns data whose timestamp is equal or later "since"`: {
				when{
					// later than oldTimestamp and earlier than new Timesamp
					updatedSince: &dummyUpdatedSinceA,
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-new-aborting"),
						Padding36("knit-a-and-b-new-completing"),
						Padding36("knit-a-and-b-new-deactivated"),
						Padding36("knit-a-and-b-new-done"),
						Padding36("knit-a-and-b-new-failed"),
						Padding36("knit-a-and-b-new-invalidated"),
						Padding36("knit-a-and-b-new-ready"),
						Padding36("knit-a-and-b-new-running"),
						Padding36("knit-a-and-b-new-starting"),
						Padding36("knit-a-and-b-new-waiting"),
						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-done"),
						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),
						Padding36("knit-no-tags-new-aborting"),
						Padding36("knit-no-tags-new-completing"),
						Padding36("knit-no-tags-new-deactivated"),
						Padding36("knit-no-tags-new-done"),
						Padding36("knit-no-tags-new-failed"),
						Padding36("knit-no-tags-new-invalidated"),
						Padding36("knit-no-tags-new-ready"),
						Padding36("knit-no-tags-new-running"),
						Padding36("knit-no-tags-new-starting"),
						Padding36("knit-no-tags-new-waiting"),
					},
				},
			},
			`when querying by "since" and "until", it returns data whose timestamp is equal or later than "since" and earlier than "until" `: {
				when{
					updatedSince: &dummyUpdatedSinceB,
					updatedUntil: &dummyUpdatedUntilB,
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-done"),
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),
						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-done"),
						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),
						Padding36("knit-no-tags-old-aborting"),
						Padding36("knit-no-tags-old-completing"),
						Padding36("knit-no-tags-old-deactivated"),
						Padding36("knit-no-tags-old-done"),
						Padding36("knit-no-tags-old-failed"),
						Padding36("knit-no-tags-old-invalidated"),
						Padding36("knit-no-tags-old-ready"),
						Padding36("knit-no-tags-old-running"),
						Padding36("knit-no-tags-old-starting"),
						Padding36("knit-no-tags-old-waiting"),
					},
				},
			},
			`when querying "since", it returns Data's run is updated after that`: {
				when{
					updatedSince: &runTimestamp,
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-done"),
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),
						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-done"),
						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),
						Padding36("knit-no-tags-old-aborting"),
						Padding36("knit-no-tags-old-completing"),
						Padding36("knit-no-tags-old-deactivated"),
						Padding36("knit-no-tags-old-done"),
						Padding36("knit-no-tags-old-failed"),
						Padding36("knit-no-tags-old-invalidated"),
						Padding36("knit-no-tags-old-ready"),
						Padding36("knit-no-tags-old-running"),
						Padding36("knit-no-tags-old-starting"),
						Padding36("knit-no-tags-old-waiting"),

						Padding36("knit-a-and-b-new-aborting"),
						Padding36("knit-a-and-b-new-completing"),
						Padding36("knit-a-and-b-new-deactivated"),
						Padding36("knit-a-and-b-new-done"),
						Padding36("knit-a-and-b-new-failed"),
						Padding36("knit-a-and-b-new-invalidated"),
						Padding36("knit-a-and-b-new-ready"),
						Padding36("knit-a-and-b-new-running"),
						Padding36("knit-a-and-b-new-starting"),
						Padding36("knit-a-and-b-new-waiting"),
						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-done"),
						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),
						Padding36("knit-no-tags-new-aborting"),
						Padding36("knit-no-tags-new-completing"),
						Padding36("knit-no-tags-new-deactivated"),
						Padding36("knit-no-tags-new-done"),
						Padding36("knit-no-tags-new-failed"),
						Padding36("knit-no-tags-new-invalidated"),
						Padding36("knit-no-tags-new-ready"),
						Padding36("knit-no-tags-new-running"),
						Padding36("knit-no-tags-new-starting"),
						Padding36("knit-no-tags-new-waiting"),

						Padding36("knit-a-and-b-no-time-aborting"),
						Padding36("knit-a-and-b-no-time-completing"),
						Padding36("knit-a-and-b-no-time-deactivated"),
						Padding36("knit-a-and-b-no-time-done"),
						Padding36("knit-a-and-b-no-time-failed"),
						Padding36("knit-a-and-b-no-time-invalidated"),
						Padding36("knit-a-and-b-no-time-ready"),
						Padding36("knit-a-and-b-no-time-running"),
						Padding36("knit-a-and-b-no-time-starting"),
						Padding36("knit-a-and-b-no-time-waiting"),
						Padding36("knit-b-and-c-no-time-aborting"),
						Padding36("knit-b-and-c-no-time-completing"),
						Padding36("knit-b-and-c-no-time-deactivated"),
						Padding36("knit-b-and-c-no-time-done"),
						Padding36("knit-b-and-c-no-time-failed"),
						Padding36("knit-b-and-c-no-time-invalidated"),
						Padding36("knit-b-and-c-no-time-ready"),
						Padding36("knit-b-and-c-no-time-running"),
						Padding36("knit-b-and-c-no-time-starting"),
						Padding36("knit-b-and-c-no-time-waiting"),
						Padding36("knit-no-tags-no-time-aborting"),
						Padding36("knit-no-tags-no-time-completing"),
						Padding36("knit-no-tags-no-time-deactivated"),
						Padding36("knit-no-tags-no-time-done"),
						Padding36("knit-no-tags-no-time-failed"),
						Padding36("knit-no-tags-no-time-invalidated"),
						Padding36("knit-no-tags-no-time-ready"),
						Padding36("knit-no-tags-no-time-running"),
						Padding36("knit-no-tags-no-time-starting"),
						Padding36("knit-no-tags-no-time-waiting"),
					},
				},
			},
			`when querying "since" and "until", it returns Data's run is updated between them`: {
				when{
					updatedSince: &runTimestamp,
					updatedUntil: &dummyUpdatedSinceA,
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-done"),
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),
						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-done"),
						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),
						Padding36("knit-no-tags-old-aborting"),
						Padding36("knit-no-tags-old-completing"),
						Padding36("knit-no-tags-old-deactivated"),
						Padding36("knit-no-tags-old-done"),
						Padding36("knit-no-tags-old-failed"),
						Padding36("knit-no-tags-old-invalidated"),
						Padding36("knit-no-tags-old-ready"),
						Padding36("knit-no-tags-old-running"),
						Padding36("knit-no-tags-old-starting"),
						Padding36("knit-no-tags-old-waiting"),

						Padding36("knit-a-and-b-no-time-aborting"),
						Padding36("knit-a-and-b-no-time-completing"),
						Padding36("knit-a-and-b-no-time-deactivated"),
						Padding36("knit-a-and-b-no-time-done"),
						Padding36("knit-a-and-b-no-time-failed"),
						Padding36("knit-a-and-b-no-time-invalidated"),
						Padding36("knit-a-and-b-no-time-ready"),
						Padding36("knit-a-and-b-no-time-running"),
						Padding36("knit-a-and-b-no-time-starting"),
						Padding36("knit-a-and-b-no-time-waiting"),
						Padding36("knit-b-and-c-no-time-aborting"),
						Padding36("knit-b-and-c-no-time-completing"),
						Padding36("knit-b-and-c-no-time-deactivated"),
						Padding36("knit-b-and-c-no-time-done"),
						Padding36("knit-b-and-c-no-time-failed"),
						Padding36("knit-b-and-c-no-time-invalidated"),
						Padding36("knit-b-and-c-no-time-ready"),
						Padding36("knit-b-and-c-no-time-running"),
						Padding36("knit-b-and-c-no-time-starting"),
						Padding36("knit-b-and-c-no-time-waiting"),
						Padding36("knit-no-tags-no-time-aborting"),
						Padding36("knit-no-tags-no-time-completing"),
						Padding36("knit-no-tags-no-time-deactivated"),
						Padding36("knit-no-tags-no-time-done"),
						Padding36("knit-no-tags-no-time-failed"),
						Padding36("knit-no-tags-no-time-invalidated"),
						Padding36("knit-no-tags-no-time-ready"),
						Padding36("knit-no-tags-no-time-running"),
						Padding36("knit-no-tags-no-time-starting"),
						Padding36("knit-no-tags-no-time-waiting"),
					},
				},
			},

			// combination
			`when querying by user tag & "knit#transient: failed", it returns data which have all of the tags`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
						tagsetAll[1],
					},
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-failed"),
						Padding36("knit-a-and-b-old-invalidated"),

						Padding36("knit-b-and-c-old-failed"),
						Padding36("knit-b-and-c-old-invalidated"),

						Padding36("knit-a-and-b-new-failed"),
						Padding36("knit-a-and-b-new-invalidated"),

						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),

						Padding36("knit-a-and-b-no-time-failed"),
						Padding36("knit-a-and-b-no-time-invalidated"),

						Padding36("knit-b-and-c-no-time-failed"),
						Padding36("knit-b-and-c-no-time-invalidated"),
					},
				},
			},
			`when querying by user tag & "knit#transient: processing", it returns data which have all of the tags`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
						tagsetAll[1],
					},
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-old-aborting"),
						Padding36("knit-a-and-b-old-completing"),
						Padding36("knit-a-and-b-old-deactivated"),
						Padding36("knit-a-and-b-old-ready"),
						Padding36("knit-a-and-b-old-running"),
						Padding36("knit-a-and-b-old-starting"),
						Padding36("knit-a-and-b-old-waiting"),

						Padding36("knit-b-and-c-old-aborting"),
						Padding36("knit-b-and-c-old-completing"),
						Padding36("knit-b-and-c-old-deactivated"),
						Padding36("knit-b-and-c-old-ready"),
						Padding36("knit-b-and-c-old-running"),
						Padding36("knit-b-and-c-old-starting"),
						Padding36("knit-b-and-c-old-waiting"),

						Padding36("knit-a-and-b-new-aborting"),
						Padding36("knit-a-and-b-new-completing"),
						Padding36("knit-a-and-b-new-deactivated"),
						Padding36("knit-a-and-b-new-ready"),
						Padding36("knit-a-and-b-new-running"),
						Padding36("knit-a-and-b-new-starting"),
						Padding36("knit-a-and-b-new-waiting"),

						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),

						Padding36("knit-a-and-b-no-time-aborting"),
						Padding36("knit-a-and-b-no-time-completing"),
						Padding36("knit-a-and-b-no-time-deactivated"),
						Padding36("knit-a-and-b-no-time-ready"),
						Padding36("knit-a-and-b-no-time-running"),
						Padding36("knit-a-and-b-no-time-starting"),
						Padding36("knit-a-and-b-no-time-waiting"),

						Padding36("knit-b-and-c-no-time-aborting"),
						Padding36("knit-b-and-c-no-time-completing"),
						Padding36("knit-b-and-c-no-time-deactivated"),
						Padding36("knit-b-and-c-no-time-ready"),
						Padding36("knit-b-and-c-no-time-running"),
						Padding36("knit-b-and-c-no-time-starting"),
						Padding36("knit-b-and-c-no-time-waiting"),
					},
				},
			},
			`when querying by user tag & "knit#timestamp: ...", it returns data which have all of the tags`: {
				when{
					tags: []kdb.Tag{
						kdb.NewTimestampTag(newTimestamp),
						tagsetAll[1],
					},
				},
				then{
					knitId: []string{
						Padding36("knit-a-and-b-new-aborting"),
						Padding36("knit-a-and-b-new-completing"),
						Padding36("knit-a-and-b-new-deactivated"),
						Padding36("knit-a-and-b-new-done"),
						Padding36("knit-a-and-b-new-failed"),
						Padding36("knit-a-and-b-new-invalidated"),
						Padding36("knit-a-and-b-new-ready"),
						Padding36("knit-a-and-b-new-running"),
						Padding36("knit-a-and-b-new-starting"),
						Padding36("knit-a-and-b-new-waiting"),

						Padding36("knit-b-and-c-new-aborting"),
						Padding36("knit-b-and-c-new-completing"),
						Padding36("knit-b-and-c-new-deactivated"),
						Padding36("knit-b-and-c-new-done"),
						Padding36("knit-b-and-c-new-failed"),
						Padding36("knit-b-and-c-new-invalidated"),
						Padding36("knit-b-and-c-new-ready"),
						Padding36("knit-b-and-c-new-running"),
						Padding36("knit-b-and-c-new-starting"),
						Padding36("knit-b-and-c-new-waiting"),
					},
				},
			},
			`when querying by user tag & "knit#id: ...", it returns data which have all of the tags`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitId, Value: Padding36("knit-a-and-b-new-done")},
						tagsetAll[1],
					},
				},
				then{knitId: []string{
					Padding36("knit-a-and-b-new-done"),
				}},
			},
			`when querying by user tag & "knit#id: ..." but no data have them all, it returns nothing`: {
				when{
					tags: []kdb.Tag{
						{Key: kdb.KeyKnitId, Value: Padding36("knit-a-and-b-new-done")},
						tagsetAll[2],
					},
				},
				then{knitId: []string{}}, // empty!
			},
		} {
			t.Run(name, func(t *testing.T) {
				nom := kpgnommock.New(t)
				nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
					return nil
				}
				testee := kpgdata.New(pool, kpgdata.WithNominator(nom))

				actual := try.To(testee.Find(ctx, testcase.when.tags, testcase.updatedSince, testcase.updatedUntil)).OrFatal(t)

				if !cmp.SliceEq(actual, testcase.then.knitId) {
					t.Errorf(
						"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
						actual, testcase.then.knitId,
					)
				}
			})
		}
	})

	t.Run("", func(t *testing.T) {
		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		if err := plan.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}

		// generating data and its upstream
		for tagcode, tag := range map[string][]kdb.Tag{
			"a-and-b": tagsetAandB,
			"b-and-c": tagsetBandC,
		} {
			var times map[string]*time.Time
			if tagcode == "a-and-b" {
				times = map[string]*time.Time{"old": &oldTimestamp}
			} else {
				times = map[string]*time.Time{"new": &newTimestamp}
			}
			for timecode, timestamp := range times {
				var statuses []kdb.KnitRunStatus
				if tagcode == "a-and-b" {
					statuses = []kdb.KnitRunStatus{
						// knit#transient: processing, they have tag A and B
						kdb.Deactivated, kdb.Waiting, kdb.Ready, kdb.Starting, kdb.Running, kdb.Aborting, kdb.Completing,
					}
				} else {
					statuses = []kdb.KnitRunStatus{
						// no knit#transient or knit#transient: failed, they have tag B and C
						kdb.Done, kdb.Failed, kdb.Invalidated,
					}
				}
				for _, status := range statuses {
					// status in id are cut off after the 4th letter,
					// to maintain ids shorter than 36 chars.
					runid := Padding36(fmt.Sprintf(
						"run_%s_%s_%s", tagcode, timecode, status,
					))
					knitid := Padding36(fmt.Sprintf(
						"knit-%s-%s-%s", tagcode, timecode, status,
					))
					step := tables.Step{
						Run: tables.Run{
							RunId: runid, Status: status, PlanId: Padding36("plan"),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-11T12:13:14.567+09:00")).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: knitid, VolumeRef: Padding64("#" + knitid),
								RunId: runid, OutputId: 1010, PlanId: Padding36("plan"),
							}: {
								UserTag: tag, Timestamp: timestamp,
							},
						},
					}
					if err := step.Apply(ctx, pool); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		t.Run(`when querying by user tag & "knit#transient: processing" but no data have them all, it returns nothing`, func(t *testing.T) {
			nom := kpgnommock.New(t)
			nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				return nil
			}
			testee := kpgdata.New(pool, kpgdata.WithNominator(nom))

			dummySince := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time()
			dummyUntil := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time().Add(1 * time.Hour)

			actual := try.To(testee.Find(
				ctx,
				[]kdb.Tag{
					{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
					tagsetAll[2],
				},
				&dummySince,
				&dummyUntil,
			)).OrFatal(t)

			expected := []string{} // empty!
			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		})

		t.Run(`when querying by user tag & "knit#transient: failed" but no data have them all, it returns nothing`, func(t *testing.T) {
			nom := kpgnommock.New(t)
			nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				return nil
			}
			testee := kpgdata.New(pool, kpgdata.WithNominator(nom))

			dummySince := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time()
			dummyUntil := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time().Add(1 * time.Hour)

			actual := try.To(testee.Find(
				ctx,
				[]kdb.Tag{
					{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
					tagsetAll[0],
				},
				&dummySince,
				&dummyUntil,
			)).OrFatal(t)

			expected := []string{} // empty!
			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		})

		t.Run(`when querying by user tag & "knit#timestamp: ..." but no data have them all, it returns nothing`, func(t *testing.T) {
			nom := kpgnommock.New(t)
			nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				return nil
			}
			testee := kpgdata.New(pool, kpgdata.WithNominator(nom))

			dummySince := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time()
			dummyUntil := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time().Add(1 * time.Hour)

			actual := try.To(testee.Find(
				ctx,
				[]kdb.Tag{
					kdb.NewTimestampTag(oldTimestamp),
					tagsetAll[2],
				},
				&dummySince,
				&dummyUntil,
			)).OrFatal(t)

			expected := []string{} // empty!
			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		})
	})

	t.Run(`let given runs are succeeded`, func(t *testing.T) {
		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		if err := plan.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}

		// generating data and its upstream
		for tagcode, tag := range map[string][]kdb.Tag{
			"a-and-b": tagsetAandB,
			"b-and-c": tagsetBandC,
			"no-tags": {},
		} {
			for timecode, timestamp := range map[string]*time.Time{
				"old":     &oldTimestamp,
				"new":     &newTimestamp,
				"no-time": nil,
			} {
				for _, status := range []kdb.KnitRunStatus{kdb.Done} {
					// status in id are cut off after the 4th letter,
					// to maintain ids shorter than 36 chars.
					runid := Padding36(fmt.Sprintf(
						"run-%s-%s-%s", tagcode, timecode, status[:4],
					))
					knitid := Padding36(fmt.Sprintf(
						"knit-%s-%s-%s", tagcode, timecode, status[:4],
					))
					step := tables.Step{
						Run: tables.Run{
							RunId: runid, Status: status, PlanId: Padding36("plan"),
							UpdatedAt: try.To(rfctime.ParseRFC3339DateTime("2022-10-11T12:13:14.567+09:00")).OrFatal(t).Time(),
						},
						Outcomes: map[tables.Data]tables.DataAttibutes{
							{
								KnitId: knitid, VolumeRef: Padding64("#" + knitid),
								RunId: runid, OutputId: 1010, PlanId: Padding36("plan"),
							}: {
								UserTag: tag, Timestamp: timestamp,
							},
						},
					}
					if err := step.Apply(ctx, pool); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		t.Run(`when querying by "knit#transient: processing", it returns nothing`, func(t *testing.T) {
			nom := kpgnommock.New(t)
			nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				return nil
			}
			testee := kpgdata.New(pool, kpgdata.WithNominator(nom))

			dummySince := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time()
			dummyUntil := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time().Add(1 * time.Hour)

			actual := try.To(testee.Find(
				ctx,
				[]kdb.Tag{
					{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientProcessing},
				},
				&dummySince,
				&dummyUntil,
			)).OrFatal(t)

			expected := []string{} // empty!
			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		})
		t.Run(`when querying by "knit#transient: failed", it returns nothing`, func(t *testing.T) {
			nom := kpgnommock.New(t)
			nom.Impl.NominateData = func(ctx context.Context, conn kpool.Tx, knitIds []string) error {
				return nil
			}
			testee := kpgdata.New(pool, kpgdata.WithNominator(nom))

			dummySince := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time()
			dummyUntil := try.To(rfctime.ParseRFC3339DateTime(
				"2022-10-11T12:13:14.567+09:00",
			)).OrFatal(t).Time().Add(1 * time.Hour)

			actual := try.To(testee.Find(
				ctx,
				[]kdb.Tag{
					{Key: kdb.KeyKnitTransient, Value: kdb.ValueKnitTransientFailed},
				},
				&dummySince,
				&dummyUntil,
			)).OrFatal(t)

			expected := []string{} // empty!
			if !cmp.SliceContentEq(actual, expected) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, expected,
				)
			}
		})
	})
}

func TestNewAgent(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	knitId := Padding36("test-knit-done")
	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: "test-plan", Active: true, Hash: "#test-plan"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: "test-plan", Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{PlanId: "test-plan", OutputId: 1_010, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-done", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: knitId, VolumeRef: "#test-knit-done",
						PlanId: "test-plan", RunId: "test-run-done", OutputId: 1_010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "tag-a", Value: "a-value"},
							{Key: "tag-b", Value: "b-value"},
						},
						Timestamp: ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
		},
	}

	type When struct {
		Mode             kdb.DataAgentMode
		LifecycleSuspend time.Duration
	}
	{
		theorySingleAgent := func(when When) func(*testing.T) {
			return func(t *testing.T) {
				ctx := context.Background()
				pool := poolBroaker.GetPool(ctx, t)

				if err := given.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}

				testee := kpgdata.New(pool)

				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()
				before := try.To(PGNow(ctx, conn)).OrFatal(t)
				agent, err := testee.NewAgent(ctx, knitId, when.Mode, when.LifecycleSuspend)
				if err != nil {
					t.Fatal(err)
				}
				after := try.To(PGNow(ctx, conn)).OrFatal(t)

				expectedAgentNamePrefix := fmt.Sprintf("knitid-%s-%s-", knitId, when.Mode)

				{
					expected := struct {
						NamePrefix   string
						Mode         kdb.DataAgentMode
						KnitDataBody kdb.KnitDataBody
					}{
						NamePrefix: expectedAgentNamePrefix,
						Mode:       when.Mode,
						KnitDataBody: kdb.KnitDataBody{
							KnitId: knitId, VolumeRef: "#test-knit-done",
							Tags: kdb.NewTagSet([]kdb.Tag{
								{Key: "tag-a", Value: "a-value"},
								{Key: "tag-b", Value: "b-value"},
								{Key: kdb.KeyKnitId, Value: knitId},
								{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:14.567+09:00"},
							}),
						},
					}
					if !strings.HasPrefix(agent.Name, expected.NamePrefix) ||
						agent.Mode != expected.Mode ||
						!agent.KnitDataBody.Equal(&expected.KnitDataBody) {
						t.Errorf(
							"unmatch DataAgent:\n===actual===\n%+v\n===expected===\n%+v",
							agent, expected,
						)
					}
				}

				{
					actual := try.To(scanner.New[tables.DataAgent]().QueryAll(
						ctx, conn,
						`select * from "data_agent" where "knit_id" = $1`,
						knitId,
					)).OrFatal(t)
					expected := []matcher.DataAgentMatcher{
						{
							Name:   matcher.Prefix(expectedAgentNamePrefix),
							KnitId: matcher.EqEq(knitId),
							Mode:   matcher.EqEq(when.Mode.String()),
							LifecycleSuspendUntil: matcher.Between(
								before, after.Add(when.LifecycleSuspend),
							),
						},
					}

					if !cmp.SliceContentEqWith(expected, actual, matcher.DataAgentMatcher.Match) {
						t.Errorf(
							"unmatch DataAgent:\n===actual===\n%+v\n===expected===\n%+v",
							actual, expected,
						)
					}
				}
			}
		}

		t.Run("when mode is 'read'", theorySingleAgent(When{
			Mode:             kdb.DataAgentRead,
			LifecycleSuspend: 30 * time.Second,
		}))

		t.Run("when mode is 'write'", theorySingleAgent(When{
			Mode:             kdb.DataAgentWrite,
			LifecycleSuspend: 30 * time.Minute,
		}))

	}

	t.Run("when create new agent for not-existing data, it should error ErrMissing", func(t *testing.T) {
		ctx := context.Background()
		pool := poolBroaker.GetPool(ctx, t)

		testee := kpgdata.New(pool)

		_, err := testee.NewAgent(ctx, Padding36("not-existing-knit"), kdb.DataAgentRead, 30*time.Second)
		if !errors.Is(err, kdb.ErrMissing) {
			t.Errorf("unexpected error: %v", err)
		}
	})

	{
		type Condition struct {
			when      When
			wantError bool
		}
		theoryMultipleAgents := func(testcase []Condition) func(*testing.T) {
			return func(t *testing.T) {
				ctx := context.Background()
				pool := poolBroaker.GetPool(ctx, t)

				if err := given.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}

				testee := kpgdata.New(pool)

				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				for nth, item := range testcase {
					_, err := testee.NewAgent(
						ctx, knitId, item.when.Mode, item.when.LifecycleSuspend,
					)
					if item.wantError {
						if err == nil {
							t.Errorf("#%d: expected error but got nil", nth+1)
						}
					} else {
						if err != nil {
							t.Errorf("#%d: unexpected error: %v", nth+1, err)
						}
					}
				}
			}
		}

		t.Run("when create many 'read' agents, it should success", theoryMultipleAgents([]Condition{
			{when: When{Mode: kdb.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: kdb.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: kdb.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
		}))

		t.Run("when create many 'write' agents, it should error", theoryMultipleAgents([]Condition{
			{when: When{Mode: kdb.DataAgentWrite, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: kdb.DataAgentWrite, LifecycleSuspend: 30 * time.Second}, wantError: true},
		}))

		t.Run("when create many 'write' and 'read' agents, it should error", theoryMultipleAgents([]Condition{
			{when: When{Mode: kdb.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: kdb.DataAgentWrite, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: kdb.DataAgentRead, LifecycleSuspend: 30 * time.Second}},
			{when: When{Mode: kdb.DataAgentWrite, LifecycleSuspend: 30 * time.Second}, wantError: true},
		}))
	}

}

func TestRemoveAgent(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	knitId := Padding36("test-knit-done")
	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: "test-plan", Active: true, Hash: "#test-plan"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: "test-plan", Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{PlanId: "test-plan", OutputId: 1_010, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-done", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: knitId, VolumeRef: "#test-knit-done",
						PlanId: "test-plan", RunId: "test-run-done", OutputId: 1_010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "tag-a", Value: "a-value"},
							{Key: "tag-b", Value: "b-value"},
						},
						Timestamp: ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:14.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
		},
	}

	type Record struct {
		Name                  string
		KnitId                string
		Mode                  string
		LifecycleSuspendUntil time.Time
	}

	type When struct {
		Name string
	}
	type Then struct {
		Names []string
		Error error
	}

	type Testcase struct {
		Given []Record
		When  When
		Then  Then
	}

	theory := func(testcase Testcase) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			for _, item := range testcase.Given {
				try.To(conn.Exec(
					ctx,
					`
						insert into "data_agent"
						("name", "mode", "knit_id", "lifecycle_suspend_until")
						values ($1, $2, $3, $4)
						`,
					item.Name, item.Mode, item.KnitId, item.LifecycleSuspendUntil,
				)).OrFatal(t)
			}

			testee := kpgdata.New(pool)

			err := testee.RemoveAgent(ctx, testcase.When.Name)
			if testcase.Then.Error != nil {
				if errors.Is(err, testcase.Then.Error) {
					t.Errorf(
						"unexpected error:\n===actual===\n%+v\n===expected===\n%+v",
						err, testcase.Then.Error,
					)
				}
			} else if err != nil {
				t.Fatal(err)
			}

			actual := try.To(scanner.New[string]().QueryAll(
				ctx, conn,
				`select "name" from "data_agent"`,
			)).OrFatal(t)

			if !cmp.SliceContentEq(
				actual, testcase.Then.Names,
			) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, testcase.Then.Names,
				)
			}
		}
	}

	t.Run("when there is no such agent, it does nothing", theory(Testcase{
		Given: []Record{
			{
				Name:                  "agent-a",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
			{
				Name:                  "agent-b",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
		},
		When: When{Name: "no-such-agent"},
		Then: Then{
			Names: []string{"agent-a", "agent-b"},
		},
	}))

	t.Run("when there are an agent with specified name, it removes the record", theory(Testcase{
		Given: []Record{
			{
				Name:                  "agent-a",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
			{
				Name:                  "agent-b",
				KnitId:                knitId,
				Mode:                  string(kdb.DataAgentRead),
				LifecycleSuspendUntil: time.Now().Add(30 * time.Second),
			},
		},
		When: When{Name: "agent-a"},
		Then: Then{
			Names: []string{"agent-b"},
		},
	}))

}

func TestPickAndRemoveAgent(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)

	knitId := Padding36("test-knit-done")
	fixture := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: "test-plan", Active: true, Hash: "#test-plan"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: "test-plan", Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{PlanId: "test-plan", OutputId: 1_010, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-done", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: knitId, VolumeRef: "#test-knit-done",
						PlanId: "test-plan", RunId: "test-run-done", OutputId: 1_010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "tag-a", Value: "a-value"},
						},
						Timestamp: ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:15.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
		},
	}

	type RecordSpec struct {
		Name     string
		Mode     kdb.DataAgentMode
		KnitId   string
		Debounce time.Duration
	}

	type Given struct {
		Agents []RecordSpec
		Locked []string
	}

	type When struct {
		Cursor            kdb.DataAgentCursor
		DoRemove          bool
		ErrorFromCallback error
	}

	type Then struct {
		LockedNames  []string
		DataAgent    kdb.DataAgent
		WantCallback bool
		Cursor       kdb.DataAgentCursor
		Error        error
		Records      []RecordSpec
	}

	theory := func(given Given, when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := fixture.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			wpool := proxy.Wrap(pool)
			wpool.Events().Query.After(func() {
				conn := try.To(pool.Acquire(ctx)).OrFatal(t)
				defer conn.Release()

				unlockedNames := try.To(
					scanner.New[string]().QueryAll(
						ctx, conn,
						`select "name" from "data_agent" for update skip locked`,
					),
				).OrFatal(t)

				for _, nameActual := range unlockedNames {
					for _, nameNotExpected := range then.LockedNames {
						if nameActual != nameNotExpected {
							continue
						}
						t.Errorf(
							"lock is not enough:\n===not locked===\n%+v\n===should be locked===\n%+v",
							unlockedNames, then.LockedNames,
						)
						return
					}
				}
			})
			testee := kpgdata.New(wpool)

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()

			before := try.To(PGNow(ctx, conn)).OrFatal(t)
			for _, item := range given.Agents {
				_, err := conn.Exec(
					ctx,
					`
					insert into "data_agent"
					("name", "mode", "knit_id", "lifecycle_suspend_until")
					values ($1, $2, $3, now() + $4)
					`,
					item.Name, item.Mode, item.KnitId, item.Debounce,
				)
				if err != nil {
					t.Fatal(err)
				}
			}
			if 0 < len(given.Locked) {
				tx := try.To(pool.Begin(ctx)).OrFatal(t)
				defer tx.Rollback(ctx)
				locked := try.To(scanner.New[string]().QueryAll(
					ctx, tx,
					`
					select "name" from "data_agent"
					where "name" = any($1::varchar[])
					for update
					`,
					given.Locked,
				)).OrFatal(t)
				if !cmp.SliceContentEq(locked, given.Locked) {
					t.Fatalf("failed to lock: got %+v, want %+v", locked, given.Locked)
				}
			}

			called := false
			actualCursor, err := testee.PickAndRemoveAgent(
				ctx, when.Cursor,
				func(da kdb.DataAgent) (bool, error) {
					called = true
					if !then.DataAgent.Equal(&da) {
						t.Errorf(
							"picked DataAgent:\n===actual===\n%+v\n===expected===\n%+v",
							da, then.DataAgent,
						)
					}
					return when.DoRemove, when.ErrorFromCallback
				},
			)
			after := try.To(PGNow(ctx, conn)).OrFatal(t)

			if then.Error != nil {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if actualCursor != then.Cursor {
				t.Errorf(
					"Cursor:\n===actual===\n%+v\n===expected===\n%+v",
					actualCursor, then.Cursor,
				)
			}

			if called != then.WantCallback {
				t.Errorf(
					"calling callback (true=want, false=not want): actual=%v, expected=%v",
					called, then.WantCallback,
				)
			}

			expectedRecords := utils.Map(
				then.Records,
				func(r RecordSpec) matcher.DataAgentMatcher {
					return matcher.DataAgentMatcher{
						Name:   matcher.EqEq(r.Name),
						Mode:   matcher.EqEq(string(r.Mode)),
						KnitId: matcher.EqEq(r.KnitId),
						LifecycleSuspendUntil: matcher.Between(
							before.Add(r.Debounce), after.Add(r.Debounce),
						),
					}
				},
			)

			records := try.To(scanner.New[tables.DataAgent]().QueryAll(
				ctx, conn, `select * from "data_agent"`,
			)).OrFatal(t)

			if !cmp.SliceContentEqWith(
				expectedRecords, records, matcher.DataAgentMatcher.Match,
			) {
				t.Errorf(
					"Records:\n===actual===\n%+v\n===expected===\n%+v",
					records, expectedRecords,
				)
			}
		}
	}

	t.Run("Let no DataAgents are given, it do nothing", theory(
		Given{},
		When{
			Cursor: kdb.DataAgentCursor{},
		},
		Then{
			WantCallback: false,
			Cursor:       kdb.DataAgentCursor{},
		},
	))

	t.Run("Let DataAgents are given, it picks a unsuspended DataAgent named next of Cursor Head, and remove", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: nil,
		},
		Then{
			LockedNames:  []string{"knitid-test-knit-done-read-14"},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-14",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				// {KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, LifecycleSuspendUntilDelta: -20 * time.Second},  // removed
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	t.Run("Let DataAgents are given and some records are locked, it picks a unsuspended & unlocked DataAgent named next of Cursor Head, and remove", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
			Locked: []string{"knitid-test-knit-done-read-14"},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: nil,
		},
		Then{
			LockedNames: []string{
				"knitid-test-knit-done-read-14", // by given precondition
				"knitid-test-knit-done-read-16", // by testee
			},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-16",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				// {KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, LifecycleSuspendUntilDelta: -10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	t.Run("Let DataAgents are given but all of them are suspended, it does not pick", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: nil,
		},
		Then{
			WantCallback: false,
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	t.Run("Let DataAgents are given, it picks a unsuspended DataAgent named next of Cursor Head, and suspend when it is not deleted", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          false,
			ErrorFromCallback: nil,
		},
		Then{
			LockedNames:  []string{"knitid-test-knit-done-read-14"},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-14",
				Debounce: time.Hour,
			},
			Error: nil,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: time.Hour},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))

	expectedError := errors.New("fake error")
	t.Run("Let DataAgents are given, it picks a unsuspended DataAgent named next of Cursor Head, and debounce it when callabck errors", theory(
		Given{
			Agents: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: -20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
		When{
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-12",
				Debounce: time.Hour,
			},
			DoRemove:          true,
			ErrorFromCallback: expectedError,
		},
		Then{
			LockedNames:  []string{"knitid-test-knit-done-read-14"},
			WantCallback: true,
			DataAgent: kdb.DataAgent{
				Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead,
				KnitDataBody: kdb.KnitDataBody{
					KnitId: knitId, VolumeRef: "#test-knit-done",
					Tags: kdb.NewTagSet([]kdb.Tag{
						{Key: "tag-a", Value: "a-value"},
						{Key: kdb.KeyKnitId, Value: knitId},
						{Key: kdb.KeyKnitTimestamp, Value: "2022-10-11T12:13:15.567+09:00"},
					}),
				},
			},
			Cursor: kdb.DataAgentCursor{
				Head:     "knitid-test-knit-done-read-14",
				Debounce: time.Hour,
			},
			Error: expectedError,
			Records: []RecordSpec{
				{KnitId: knitId, Name: "knitid-test-knit-done-read-12", Mode: kdb.DataAgentRead, Debounce: -30 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-14", Mode: kdb.DataAgentRead, Debounce: time.Hour},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-16", Mode: kdb.DataAgentRead, Debounce: -10 * time.Second}, // ^^^ past
				{KnitId: knitId, Name: "knitid-test-knit-done-read-18", Mode: kdb.DataAgentRead, Debounce: 10 * time.Second},  // vvv future
				{KnitId: knitId, Name: "knitid-test-knit-done-read-20", Mode: kdb.DataAgentRead, Debounce: 20 * time.Second},
				{KnitId: knitId, Name: "knitid-test-knit-done-read-22", Mode: kdb.DataAgentRead, Debounce: 30 * time.Second},
			},
		},
	))
}

func TestGetAgentName(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: "test-plan", Active: true, Hash: "#test-plan"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: "test-plan", Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{PlanId: "test-plan", OutputId: 1_010, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-1", Status: kdb.Running,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-1"), VolumeRef: "#test-knit-running-1",
						PlanId: "test-plan", RunId: "test-run-running-1", OutputId: 1_010,
					}: {
						Agent: []tables.DataAgent{
							{
								Name: "test-agent-1", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:14.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name: "test-agent-2", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:15.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name: "test-agent-3", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:16.567+09:00",
								)).OrFatal(t).Time(),
							},
							{
								Name: "test-agent-4", Mode: kdb.DataAgentWrite.String(),
								KnitId: Padding36("test-knit-running-1"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:17.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-2", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-2"), VolumeRef: "#test-knit-running-2",
						PlanId: "test-plan", RunId: "test-run-running-2", OutputId: 1_010,
					}: {},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-3", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-3"), VolumeRef: "#test-knit-running-3",
						PlanId: "test-plan", RunId: "test-run-running-3", OutputId: 1_010,
					}: {
						Agent: []tables.DataAgent{
							{
								Name: "test-agent-5", Mode: kdb.DataAgentWrite.String(),
								KnitId: Padding36("test-knit-running-3"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:17.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-4", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-4"), VolumeRef: "#test-knit-running-4",
						PlanId: "test-plan", RunId: "test-run-running-4", OutputId: 1_010,
					}: {
						Agent: []tables.DataAgent{
							{
								Name: "test-agent-6", Mode: kdb.DataAgentRead.String(),
								KnitId: Padding36("test-knit-running-4"),
								LifecycleSuspendUntil: try.To(rfctime.ParseRFC3339DateTime(
									"2022-10-11T12:13:18.567+09:00",
								)).OrFatal(t).Time(),
							},
						},
					},
				},
			},
		},
	}

	type When struct {
		KnitId string
		Mode   []kdb.DataAgentMode
	}

	type Then struct {
		Names []string
		Error error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			testee := kpgdata.New(pool)

			actual, err := testee.GetAgentName(ctx, when.KnitId, when.Mode)

			if then.Error != nil {
				if !errors.Is(err, then.Error) {
					t.Errorf("expected error %v but got %v", then.Error, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !cmp.SliceContentEq(actual, then.Names) {
				t.Errorf(
					"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
					actual, then.Names,
				)
			}
		}
	}

	t.Run("when there is no such data, it returns Missing error", theory(
		When{
			KnitId: Padding36("no-such-knit"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentRead, kdb.DataAgentWrite},
		},
		Then{
			Names: []string{},
			Error: kdb.ErrMissing,
		},
	))

	t.Run("when there is no such agent, it returns empty", theory(
		When{
			KnitId: Padding36("test-knit-running-2"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentRead, kdb.DataAgentWrite},
		},
		Then{
			Names: []string{},
			Error: nil,
		},
	))

	t.Run("when there is an agent with specified mode (read), it returns the name", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentRead},
		},
		Then{
			Names: []string{"test-agent-1", "test-agent-2", "test-agent-3"},
			Error: nil,
		},
	))

	t.Run("when there is an agent with specified mode (write), it returns the name", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Mode:   []kdb.DataAgentMode{kdb.DataAgentWrite},
		},
		Then{
			Names: []string{"test-agent-4"},
			Error: nil,
		},
	))
}

func TestUpdateTag(t *testing.T) {
	poolBroaker := testenv.NewPoolBroaker(context.Background(), t)
	given := tables.Operation{
		Plan: []tables.Plan{
			{PlanId: "test-plan", Active: true, Hash: "#test-plan"},
		},
		PlanPseudo: []tables.PlanPseudo{
			{PlanId: "test-plan", Name: "pseudo"},
		},
		Outputs: map[tables.Output]tables.OutputAttr{
			{PlanId: "test-plan", OutputId: 1_010, Path: "/out"}: {},
		},
		Steps: []tables.Step{
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-1", Status: kdb.Running,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-1"), VolumeRef: "#test-knit-running-1",
						PlanId: "test-plan", RunId: "test-run-running-1", OutputId: 1_010,
					}: {
						UserTag: []kdb.Tag{
							{Key: "tag-a", Value: "value 1"},
							{Key: "tag-a", Value: "value 2"},
							{Key: "tag-b", Value: "value 1"},
						},
						Timestamp: ref(try.To(rfctime.ParseRFC3339DateTime(
							"2022-10-11T12:13:15.567+09:00",
						)).OrFatal(t).Time()),
					},
				},
			},
			{
				Run: tables.Run{
					PlanId: "test-plan", RunId: "test-run-running-2", Status: kdb.Done,
					UpdatedAt: try.To(rfctime.ParseRFC3339DateTime(
						"2022-10-11T12:13:14.567+09:00",
					)).OrFatal(t).Time(),
				},
				Outcomes: map[tables.Data]tables.DataAttibutes{
					{
						KnitId: Padding36("test-knit-running-2"), VolumeRef: "#test-knit-running-2",
						PlanId: "test-plan", RunId: "test-run-running-2", OutputId: 1_010,
					}: {},
				},
			},
		},
	}

	type When struct {
		KnitId string
		Delta  kdb.TagDelta
	}

	type Then struct {
		KnownTags []kdb.Tag
		Tagging   map[string][]kdb.Tag // knitId -> []Tag
		Error     error
	}

	theory := func(when When, then Then) func(*testing.T) {
		return func(t *testing.T) {
			ctx := context.Background()
			pool := poolBroaker.GetPool(ctx, t)

			if err := given.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}

			testee := kpgdata.New(pool)

			err := testee.UpdateTag(ctx, when.KnitId, when.Delta)

			if then.Error != nil {
				if !errors.Is(err, then.Error) {
					t.Errorf("expected error %v but got %v", then.Error, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			conn := try.To(pool.Acquire(ctx)).OrFatal(t)
			defer conn.Release()
			{
				actual := try.To(scanner.New[kdb.Tag]().QueryAll(
					ctx, conn,
					`
					select
						"tag_key"."key" as "key",
						"tag"."value" as "value"
					from "tag"
					inner join "tag_key"
						on "tag"."key_id" = "tag_key"."id"
					`,
				)).OrFatal(t)

				if !cmp.SliceContentEqWith(
					actual, then.KnownTags,
					func(a, b kdb.Tag) bool { return a.Equal(&b) },
				) {
					t.Errorf(
						"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
						actual, then.KnownTags,
					)
				}
			}

			{
				actual := try.To(scanner.New[struct {
					KnitId string
					Key    string
					Value  string
				}]().QueryAll(
					ctx, conn,
					`
					select
						"tag_data"."knit_id" as "knit_id",
						"tag_key"."key" as "key",
						"tag"."value" as "value"
					from "tag_data"
					inner join "tag"
						on "tag_data"."tag_id" = "tag"."id"
					inner join "tag_key"
						on "tag"."key_id" = "tag_key"."id"
					`,
				)).OrFatal(t)

				tagging := map[string][]kdb.Tag{}
				for _, r := range actual {
					tagging[r.KnitId] = append(
						tagging[r.KnitId],
						kdb.Tag{Key: r.Key, Value: r.Value},
					)
				}

				if !cmp.MapEqWith(
					tagging, then.Tagging,
					func(a, b []kdb.Tag) bool {
						return cmp.SliceContentEqWith(
							a, b,
							func(a, b kdb.Tag) bool { return a.Equal(&b) },
						)
					},
				) {
					t.Errorf(
						"unmatch:\n===actual===\n%+v\n===expected===\n%+v",
						tagging, then.Tagging,
					)
				}
			}
		}
	}

	t.Run("when there is no such data, it returns Missing error", theory(
		When{
			KnitId: Padding36("no-such-knit"),
			Delta:  kdb.TagDelta{Add: []kdb.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: kdb.ErrMissing,
		},
	))

	t.Run("when adding new tag, it adds the tag", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  kdb.TagDelta{Add: []kdb.Tag{{Key: "tag-c", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
				{Key: "tag-c", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
					{Key: "tag-c", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding existing tag, it does nothing", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  kdb.TagDelta{Add: []kdb.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when removing existing tag, it removes the tag", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  kdb.TagDelta{Remove: []kdb.Tag{{Key: "tag-a", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when removing non-existing tag, it does nothing", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta:  kdb.TagDelta{Remove: []kdb.Tag{{Key: "tag-c", Value: "value 1"}}},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing tags, it adds and removes the tags", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: kdb.TagDelta{
				Add:    []kdb.Tag{{Key: "tag-c", Value: "value 1"}},
				Remove: []kdb.Tag{{Key: "tag-a", Value: "value 1"}},
			},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
				{Key: "tag-c", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
					{Key: "tag-c", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing same new tag, it results as it was", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: kdb.TagDelta{
				Add:    []kdb.Tag{{Key: "tag-a", Value: "value 3"}},
				Remove: []kdb.Tag{{Key: "tag-a", Value: "value 3"}},
			},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-a", Value: "value 3"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-a", Value: "value 2"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

	t.Run("when adding and removing same existing tag, it removes the tags", theory(
		When{
			KnitId: Padding36("test-knit-running-1"),
			Delta: kdb.TagDelta{
				Add:    []kdb.Tag{{Key: "tag-a", Value: "value 2"}},
				Remove: []kdb.Tag{{Key: "tag-a", Value: "value 2"}},
			},
		},
		Then{
			KnownTags: []kdb.Tag{
				{Key: "tag-a", Value: "value 1"},
				{Key: "tag-a", Value: "value 2"},
				{Key: "tag-b", Value: "value 1"},
			},
			Tagging: map[string][]kdb.Tag{
				Padding36("test-knit-running-1"): {
					{Key: "tag-a", Value: "value 1"},
					{Key: "tag-b", Value: "value 1"},
				},
			},
			Error: nil,
		},
	))

}

func ref[T any](t T) *T {
	return &t
}
