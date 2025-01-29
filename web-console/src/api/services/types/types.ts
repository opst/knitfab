import { Assignment, DataDetail, PlanDetail, PlanSummary, RunDetail, RunSummary, Tag } from "../../../types/types";

export type RawDataSummary = {
    knitId: string;
    tags: TagString[];
};

export type RawDataDetail = RawDataSummary & {
    upstream: RawCreatedFrom
    downstreams: RawAssignedTo[]
    nomination: RawNominatedBy[]
};

export type TagString = `${string}:${string}`;

export function isTagString(tag: string): tag is TagString {
    return tag.includes(':');
}

export function toDataDetail(data: RawDataDetail): DataDetail {

    return {
        knitId: data.knitId,
        tags: data.tags.map(parseTag),
        upstream: {
            run: toRunSummary(data.upstream.run),
            mountpoint: data.upstream.mountpoint && {
                path: data.upstream.mountpoint.path,
                tags: data.upstream.mountpoint.tags.map(parseTag)
            },
            log: data.upstream.log && {
                tags: data.upstream.log.tags.map(parseTag)
            }
        },
        downstreams: data.downstreams.map(d => ({
            mountpoint: {
                path: d.mountpoint.path,
                tags: d.mountpoint.tags.map(parseTag)
            },
            run: toRunSummary(d.run)
        })),
        nomination: data.nomination.map(n => ({
            plan: toPlanSummary(n.plan),
            tags: n.tags.map(parseTag),
            path: n.path,
        }))
    }
}

export function parseTag(tag: TagString): Tag {
    const [key, value] = tag.split(/(?<=^[^:]*):/, 2);
    return { key: key.trim(), value: value.trim() };
}

export function toTagString(tag: Tag): TagString {
    return `${tag.key}:${tag.value}`;
}

export type RawPlanSummary = {
    planId: string
    image?: string
    name?: string
    entrypoint?: string[]
    args?: string[]
    annotations?: string[]
};

export function toPlanSummary(plan: RawPlanSummary): PlanSummary {
    return {
        planId: plan.planId,
        image: plan.image,
        name: plan.name,
        entrypoint: plan.entrypoint || [],
        args: plan.args || [],
        annotations: plan.annotations || []
    }
}

export type RawPlanDetail = RawPlanSummary & {
    inputs: Input[]
    outputs?: Output[]
    log?: Log
    active: boolean
    on_node?: RawOnNode
    resources?: { [key: string]: string },
    service_account?: string,
}

export function toPlanDetail(plan: RawPlanDetail): PlanDetail {
    return {
        planId: plan.planId,
        image: plan.image,
        name: plan.name,
        entrypoint: plan.entrypoint || [],
        args: plan.args || [],
        annotations: plan.annotations || [],
        inputs: plan.inputs.map(i => ({
            path: i.path,
            tags: i.tags.map(parseTag),
            upstreams: i.upstreams.map(u => ({
                plan: toPlanSummary(u.plan),
                mountpoint: u.mountpoint && {
                    path: u.mountpoint.path,
                    tags: u.mountpoint.tags.map(parseTag)
                },
                log: u.log && {
                    tags: u.log.tags.map(parseTag)
                }
            }))
        })),
        outputs: (plan.outputs ?? []).map(o => ({
            path: o.path,
            tags: o.tags.map(parseTag),
            downstreams: o.downstreams.map(d => ({
                plan: toPlanSummary(d.plan),
                mountpoint: {
                    path: d.mountpoint.path,
                    tags: d.mountpoint.tags.map(parseTag)
                }
            }))
        })),
        log: plan.log && {
            tags: plan.log.tags.map(parseTag),
            downstreams: plan.log.downstreams.map(d => ({
                plan: toPlanSummary(d.plan),
                mountpoint: {
                    path: d.mountpoint.path,
                    tags: d.mountpoint.tags.map(parseTag)
                }
            }))
        },
        active: plan.active,
        onNode: plan.on_node,
        resources: plan.resources ?? {},
        serviceAccount: plan.service_account
    } satisfies PlanDetail
}

export type RawRunSummary = {
    runId: string
    status: string
    updatedAt: string
    exit?: Exit
    plan: RawPlanSummary
}

export function toRunSummary(run: RawRunSummary): RunSummary {
    return {
        runId: run.runId,
        status: run.status,
        updatedAt: new Date(run.updatedAt),
        exit: run.exit,
        plan: toPlanSummary(run.plan)
    }
}

export type RawRunDetail = RawRunSummary & {
    inputs: RawAssignment[]
    outputs: RawAssignment[]
    log?: RawLogSummary
}

export function toRunDetail(run: RawRunDetail): RunDetail {

    function toAssignment(assignment: RawAssignment): Assignment {
        return {
            knitId: assignment.knitId,
            path: assignment.path,
            tags: assignment.tags.map(parseTag)
        }
    }

    return {
        runId: run.runId,
        status: run.status,
        updatedAt: new Date(run.updatedAt),
        exit: run.exit,
        plan: toPlanSummary(run.plan),
        inputs: run.inputs.map(toAssignment),
        outputs: run.outputs.map(toAssignment),
        log: run.log && {
            tags: run.log.tags.map(parseTag),
            knitId: run.log.knitId,
        }
    }
}

export type RawCreatedFrom = {
    run: RawRunSummary
    mountpoint?: RawMountpoint
    log?: LogPoint
}

export type RawMountpoint = {
    path: string
    tags: TagString[]
}

export type LogPoint = {
    tags: TagString[]
}

export type RawAssignedTo = {
    mountpoint: RawMountpoint
    run: RawRunSummary
}

export type RawNominatedBy = RawMountpoint & {
    plan: RawPlanSummary
}

export type Input = RawMountpoint & {
    upstreams: RawUpstream[]
}

export type RawUpstream = {
    plan: RawPlanSummary
    mountpoint?: RawMountpoint
    log?: LogPoint
}

export type Output = RawMountpoint & {
    downstreams: Downstream[]
}

type Downstream = {
    plan: RawPlanSummary
    mountpoint: RawMountpoint
}

// Log for Plan Definition
type Log = LogPoint & {
    downstreams: Downstream[]
}

type RawOnNode = {
    may: string[]
    prefer: string[]
    must: string[]
}

type Exit = {
    code: number
    message: string
}

type RawAssignment = RawMountpoint & {
    knitId: string
}

type RawLogSummary = LogPoint & {
    knitId: string
}

export type RunStatus = "deactivated" | "waiting" | "ready" | "starting" | "running" | "completing" | "aborting" | "done" | "failed";

export const RunStatuses: RunStatus[] = [
    "deactivated",
    "waiting",
    "ready",
    "starting",
    "running",
    "completing",
    "aborting",
    "done",
    "failed"
];
