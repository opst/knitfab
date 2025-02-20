export type DataSummary = {
    knitId: string;
    tags: Tag[];
};

export type DataDetail = DataSummary & {
    upstream: CreatedFrom;
    downstreams: AssignedTo[];
    nomination: NominatedBy[];
};

export type PlanSummary = {
    planId: string
    image?: string
    name?: string
    entrypoint: string[]
    args: string[]
    annotations: string[]
};

export type PlanDetail = PlanSummary & {
    inputs: Input[]
    outputs: Output[]
    log?: Log
    active: boolean
    onNode?: OnNode
    resources: { [key: string]: string },
    serviceAccount?: string,
};

export type RunSummary = {
    runId: string
    status: string
    updatedAt: luxon.DateTime
    exit?: Exit
    plan: PlanSummary
};

export type RunDetail = RunSummary & {
    inputs: Assignment[]
    outputs: Assignment[]
    log?: LogSummary
};


export type Tag = {
    key: string
    value: string
}

export function tagsEqual(a: Tag, b: Tag): boolean {
    return a.key === b.key && a.value === b.value;
}

export type CreatedFrom = {
    run: RunSummary
    mountpoint?: Mountpoint
    log?: LogPoint
}

export type Mountpoint = {
    tags: Tag[]
    path: string
}

export type LogPoint = {
    tags: Tag[]
}

export type AssignedTo = {
    mountpoint: Mountpoint
    run: RunSummary
}

export type NominatedBy = Mountpoint & {
    plan: PlanSummary
}

export type Input = Mountpoint & {
    upstreams: Upstream[]
}

export type Upstream = {
    plan: PlanSummary
    mountpoint?: Mountpoint
    log?: LogPoint
}

export type Output = Mountpoint & {
    downstreams: Downstream[]
}

export type Downstream = {
    plan: PlanSummary
    mountpoint: Mountpoint
}

export type Assignment = Mountpoint & {
    knitId: string
}

export type Log = LogPoint & {
    downstreams: Downstream[]
}

export type LogSummary = LogPoint & {
    knitId: string
}

export type Exit = {
    code: number
    message: string
}

export type OnNode = {
    may: string[]
    prefer: string[]
    must: string[]
}
