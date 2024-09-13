package find_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/cmp"
	kdb "github.com/opst/knitfab/pkg/db"
	kpgdata "github.com/opst/knitfab/pkg/db/postgres/data"
	kpgnommock "github.com/opst/knitfab/pkg/db/postgres/nominator/mock"
	kpool "github.com/opst/knitfab/pkg/db/postgres/pool"
	testenv "github.com/opst/knitfab/pkg/db/postgres/pool/testenv"
	"github.com/opst/knitfab/pkg/db/postgres/tables"
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
