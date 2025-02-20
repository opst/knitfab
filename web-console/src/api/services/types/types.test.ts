import { act } from "react";
import { DataDetail, PlanDetail, PlanSummary } from "../../../types/types";
import {
    isTagString,
    parseTag,
    RawDataDetail,
    RawPlanDetail,
    RawPlanSummary,
    RawRunDetail,
    RawRunSummary,
    toDataDetail,
    toPlanDetail,
    toPlanSummary,
    toRunDetail,
    toRunSummary,
    toTagString,
} from "./types";
import { DateTime } from "luxon";

// isTagString
describe("isTagString", () => {
    test("should return true for valid TagString", () => {
        expect(isTagString("key:value")).toBe(true);
    });

    test("should return false for invalid TagString", () => {
        expect(isTagString("invalidTag")).toBe(false);
    });
});

// parseTag
describe("parseTag", () => {
    test("should parse TagString into an object", () => {
        expect(parseTag("key:value")).toEqual({ key: "key", value: "value" });
    });
    test("should split key and value at the first colon", () => {
        expect(parseTag("key:value:with:colon")).toEqual({ key: "key", value: "value:with:colon" });
    });
});

// toTagString
describe("toTagString", () => {
    test("should convert Tag object into TagString", () => {
        expect(toTagString({ key: "key", value: "value" })).toBe("key:value");
    });
});

// toPlanSummary
describe("toPlanSummary", () => {
    test("should convert RawPlanSummary to PlanSummary with defaults", () => {
        const rawPlan: RawPlanSummary = { planId: "123" };
        expect(toPlanSummary(rawPlan)).toEqual({
            planId: "123",
            image: undefined,
            name: undefined,
            entrypoint: [],
            args: [],
            annotations: []
        });
    });

    test("should convert RawPlanSummary to PlanSummary without modification", () => {
        const rawPlan: RawPlanSummary = {
            planId: "123",
            image: "img",
            name: "test",
            entrypoint: ["start"],
            args: ["arg1"],
            annotations: ["anno=tations"],
        } satisfies PlanSummary;
        expect(toPlanSummary(rawPlan)).toEqual(rawPlan);
    });
});

// toPlanDetail
describe("toPlanDetail", () => {
    test("should convert RawPlanDetail to PlanDetail with defaults", () => {
        const rawPlan: RawPlanDetail = {
            planId: "123",
            active: true,
            inputs: []
        };
        expect(toPlanDetail(rawPlan)).toEqual({
            planId: "123",
            image: undefined,
            name: undefined,
            entrypoint: [],
            args: [],
            annotations: [],
            inputs: [],
            outputs: [],
            log: undefined,
            active: true,
            onNode: undefined,
            resources: {},
            serviceAccount: undefined,
        } satisfies PlanDetail);
    });

    test("should convert RawPlanDetail to PlanDetail without modification", () => {
        const rawPlan: RawPlanDetail = {
            planId: "123",
            image: "img",
            name: "test",
            entrypoint: ["start"],
            args: ["arg1"],
            annotations: ["anno=tations"],
            inputs: [],
            outputs: [],
            active: true,
            on_node: { may: ["node1"], prefer: ["node2"], must: ["node3"] },
            resources: { cpu: "2" },
            service_account: "sa",
        };
        expect(toPlanDetail(rawPlan)).toEqual({
            planId: "123",
            image: "img",
            name: "test",
            entrypoint: ["start"],
            args: ["arg1"],
            annotations: ["anno=tations"],
            inputs: [],
            outputs: [],
            active: true,
            onNode: { may: ["node1"], prefer: ["node2"], must: ["node3"] },
            resources: { cpu: "2" },
            serviceAccount: "sa",
        } satisfies PlanDetail);
    });
});

describe("toRunSummary", () => {
    test("should convert RawRunSummary to RunSummary with defaults", () => {
        const rawRun: RawRunSummary = {
            runId: "run123",
            status: "running",
            updatedAt: "2024-02-10T10:00:00Z",
            plan: { planId: "123" },
        };
        expect(toRunSummary(rawRun)).toEqual({
            runId: "run123",
            status: "running",
            updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
            exit: undefined,
            plan: {
                planId: "123",
                image: undefined,
                name: undefined,
                entrypoint: [],
                args: [],
                annotations: []
            }
        });
    });

    test("should convert RawRunSummary to RunSummary without modification", () => {
        const rawRun: RawRunSummary = {
            runId: "run123",
            status: "running",
            updatedAt: "2024-02-10T10:00:00Z",
            exit: { code: 0, message: "Success" },
            plan: {
                planId: "123",
                image: "img",
                name: "test",
                entrypoint: ["start"],
                args: ["arg1"],
                annotations: ["anno=tations"],
            },
        };
        expect(toRunSummary(rawRun)).toEqual({
            runId: "run123",
            status: "running",
            updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
            exit: { code: 0, message: "Success" },
            plan: {
                planId: "123",
                image: "img",
                name: "test",
                entrypoint: ["start"],
                args: ["arg1"],
                annotations: ["anno=tations"],
            }
        });
    });
});

// toRunDetail
describe("toRunDetail", () => {
    test("should convert RawRunDetail to RunDetail with defaults", () => {
        const rawRun: RawRunDetail = {
            runId: "run123",
            status: "running",
            updatedAt: "2024-02-10T10:00:00Z",
            plan: { planId: "123" },
            inputs: [],
            outputs: [],
        };
        expect(toRunDetail(rawRun)).toEqual({
            runId: "run123",
            status: "running",
            updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
            exit: undefined,
            plan: {
                planId: "123",
                entrypoint: [],
                args: [],
                annotations: [],
            },
            inputs: [],
            outputs: [],
            log: undefined
        });
    });

    test("should convert RawRunDetail to RunDetail without modification", () => {
        const rawRun: RawRunDetail = {
            runId: "run123",
            status: "running",
            updatedAt: "2024-02-10T10:00:00Z",
            exit: { code: 0, message: "Success" },
            plan: {
                planId: "123",
                image: "img",
                name: "test",
                entrypoint: ["start"],
                args: ["arg1"],
                annotations: ["anno=tations"],
            },
            inputs: [
                {
                    path: "/in/1",
                    tags: ["key:value"],
                    knitId: "knit123",
                },
            ],
            outputs: [
                {
                    path: "/out/1",
                    tags: ["key:value"],
                    knitId: "knit456",
                },
            ],
            log: { knitId: "log123", tags: ["log:entry"] },
        };
        expect(toRunDetail(rawRun)).toEqual({
            runId: "run123",
            status: "running",
            updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
            exit: { code: 0, message: "Success" },
            plan: {
                planId: "123",
                image: "img",
                name: "test",
                entrypoint: ["start"],
                args: ["arg1"],
                annotations: ["anno=tations"],
            },
            inputs: [
                {
                    path: "/in/1",
                    tags: [{ key: "key", value: "value" }],
                    knitId: "knit123",
                },
            ],
            outputs: [
                {
                    path: "/out/1",
                    tags: [{ key: "key", value: "value" }],
                    knitId: "knit456",
                },
            ],
            log: { knitId: "log123", tags: [{ key: "log", value: "entry" }] }
        });
    });
});

describe("toDataDetail", () => {
    test("should convert RawDataDetail to DataDetail with defaults", () => {
        const rawData: RawDataDetail = {
            knitId: "knit123",
            tags: [],
            upstream: {
                run: {
                    runId: "run123",
                    status: "running",
                    updatedAt: "2024-02-10T10:00:00Z",
                    plan: { planId: "123" },
                },
                mountpoint: { path: "/path", tags: [] },
            },
            downstreams: [],
            nomination: [],
        };

        expect(toDataDetail(rawData)).toEqual({
            knitId: "knit123",
            tags: [],
            upstream: {
                run: {
                    runId: "run123",
                    status: "running",
                    updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
                    exit: undefined,
                    plan: {
                        planId: "123",
                        image: undefined,
                        name: undefined,
                        entrypoint: [],
                        args: [],
                        annotations: []
                    }
                },
                mountpoint: { path: "/path", tags: [] },
                log: undefined
            },
            downstreams: [],
            nomination: [],
        } satisfies DataDetail);
    });

    test("should convert RawDataDetail to DataDetail without modification", () => {
        const rawData: RawDataDetail = {
            knitId: "knit123",
            tags: ["key:value"],
            upstream: {
                run: {
                    runId: "run123",
                    status: "running",
                    updatedAt: "2024-02-10T10:00:00Z",
                    plan: {
                        planId: "123",
                        image: "img",
                        name: "test",
                        entrypoint: ["start"],
                        args: ["arg1"],
                        annotations: ["anno=tations"],
                    },
                },
                mountpoint: { path: "/path", tags: ["key:value"] },
                log: { tags: ["log:entry"] },
            },
            downstreams: [
                {
                    mountpoint: { path: "/path", tags: ["key:value"] },
                    run: {
                        runId: "run456",
                        status: "running",
                        updatedAt: "2024-02-10T10:00:00Z",
                        plan: {
                            planId: "456",
                            image: "img",
                            name: "test",
                            entrypoint: ["start"],
                            args: ["arg1"],
                            annotations: ["anno=tations"],
                        },
                    },
                },
            ],
            nomination: [
                {
                    plan: {
                        planId: "123",
                        image: "img",
                        name: "test",
                        entrypoint: ["start"],
                        args: ["arg1"],
                        annotations: ["anno=tations"],
                    },
                    tags: ["key:value"],
                    path: "/path",
                },
            ],
        };

        expect(toDataDetail(rawData)).toEqual({
            knitId: "knit123",
            tags: [{ key: "key", value: "value" }],
            upstream: {
                run: {
                    runId: "run123",
                    status: "running",
                    updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
                    exit: undefined,
                    plan: {
                        planId: "123",
                        image: "img",
                        name: "test",
                        entrypoint: ["start"],
                        args: ["arg1"],
                        annotations: ["anno=tations"],
                    }
                },
                mountpoint: { path: "/path", tags: [{ key: "key", value: "value" }] },
                log: { tags: [{ key: "log", value: "entry" }] },
            },
            downstreams: [
                {
                    mountpoint: { path: "/path", tags: [{ key: "key", value: "value" }] },
                    run: {
                        runId: "run456",
                        status: "running",
                        updatedAt: DateTime.fromISO("2024-02-10T10:00:00Z"),
                        exit: undefined,
                        plan: {
                            planId: "456",
                            image: "img",
                            name: "test",
                            entrypoint: ["start"],
                            args: ["arg1"],
                            annotations: ["anno=tations"],
                        }
                    }
                }
            ],
            nomination: [
                {
                    plan: {
                        planId: "123",
                        image: "img",
                        name: "test",
                        entrypoint: ["start"],
                        args: ["arg1"],
                        annotations: ["anno=tations"],
                    },
                    tags: [{ key: "key", value: "value" }],
                    path: "/path",
                },
            ],
        } satisfies DataDetail);
    });
})
