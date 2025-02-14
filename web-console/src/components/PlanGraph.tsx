import Box from "@mui/material/Box";
import {
    Background,
    Controls,
    Handle,
    Node,
    NodeProps,
    Position,
    ReactFlow,
    ReactFlowProvider,
} from "@xyflow/react";
import React, { useEffect } from "react";
import { Input, Log, LogPoint, Output, PlanDetail } from "../types/types";
import { InputPointCard, LogPointCard, OutputPointCard, PlanCard, PlanItem } from "./Items";
import { getLayoutedNodes, NodeToBeLayouted } from "../dag";
import { Collapse, Stack } from "@mui/material";
import { PlanService } from "../api/services/planService";

type PlanNodeValues = {
    plan: PlanDetail,
    onResize: (planId: string, size: { width: number, height: number }) => void,
    onClick: (plan: PlanDetail) => void,
}

const PlanNode: React.FC<NodeProps<Node<PlanNodeValues, "planNode">>> = ({ data }) => {
    const { plan, onResize, onClick } = data;

    const ref = React.useRef<HTMLElement>(null);

    useEffect(() => {
        const observer = new ResizeObserver((entries) => {
            entries.forEach((entry) => {
                const node = entry.target;
                onResize(plan.planId, { width: node.clientWidth, height: node.clientHeight })
            })
        })
        if (!ref.current) { return; }

        observer.observe(ref.current, { box: "border-box" });

        return () => { observer.disconnect(); }
    })
    return (
        <>
            {
                0 < plan.inputs.length &&
                <Handle type="target" position={Position.Top} isConnectable={false} />
            }
            <Box maxWidth="33vw" ref={ref} onClick={(ev) => {
                ev.stopPropagation();
                onClick(plan);
            }}>
                <PlanCard plan={plan} />
            </Box>
            {
                (0 < plan.outputs.length || plan.log) &&
                <Handle type="source" position={Position.Bottom} isConnectable={false} />
            }
        </>
    );
}

type InputNodeaValues = {
    planId: string,
    input: Input,
    onResize: (planId: string, path: string, size: { width: number, height: number }) => void,
}
const InputNode: React.FC<NodeProps<Node<InputNodeaValues, "inputNode">>> = ({ data }) => {
    const { planId, input, onResize } = data;

    const ref = React.useRef<HTMLElement>(null);

    useEffect(() => {
        const observer = new ResizeObserver((entries) => {
            entries.forEach((entry) => {
                const node = entry.target;
                onResize(planId, input.path, { width: node.clientWidth, height: node.clientHeight })
            })
        })
        if (!ref.current) { return; }

        observer.observe(ref.current, { box: "border-box" });

        return () => { observer.disconnect(); }
    })

    return (
        <>
            {
                0 < input.upstreams.length &&
                <Handle type="target" position={Position.Top} isConnectable={false} />
            }
            <Box maxWidth="33vw" ref={ref}>
                <InputPointCard mountpoint={input} />
            </Box>
            <Handle type="source" position={Position.Bottom} isConnectable={false} />
        </>
    );
}

type OutputNodeValues = {
    planId: string,
    output: Output,
    onResize: (planId: string, path: string, size: { width: number, height: number }) => void,
}

const OutputNode: React.FC<NodeProps<Node<OutputNodeValues, "outputNode">>> = ({ data }) => {
    const { planId, output, onResize } = data;

    const ref = React.useRef<HTMLElement>(null);

    useEffect(() => {
        const observer = new ResizeObserver((entries) => {
            entries.forEach((entry) => {
                const node = entry.target;
                onResize(planId, output.path, { width: node.clientWidth, height: node.clientHeight })
            })
        })
        if (!ref.current) { return; }

        observer.observe(ref.current, { box: "border-box" });

        return () => { observer.disconnect(); }
    })

    return (
        <>
            <Handle type="target" position={Position.Top} isConnectable={false} />
            <Box maxWidth="33vw" ref={ref}>
                <OutputPointCard mountpoint={output} />
            </Box>
            {
                0 < output.downstreams.length &&
                <Handle type="source" position={Position.Bottom} isConnectable={false} />
            }
        </>
    );
}

type LogNodeValues = {
    planId: string,
    log: Log,
    onResize: (planId: string, size: { width: number, height: number }) => void,
}

const LogNode: React.FC<NodeProps<Node<LogNodeValues, "logNode">>> = ({ data }) => {
    const { planId, log, onResize } = data;

    const ref = React.useRef<HTMLElement>(null);

    useEffect(() => {
        const observer = new ResizeObserver((entries) => {
            entries.forEach((entry) => {
                const node = entry.target;
                onResize(planId, { width: node.clientWidth, height: node.clientHeight })
            })
        })
        if (!ref.current) { return; }

        observer.observe(ref.current, { box: "border-box" });

        return () => { observer.disconnect(); }
    })

    return (
        <>
            <Handle type="target" position={Position.Top} isConnectable={false} />
            <Box maxWidth="33vw" ref={ref}>
                <LogPointCard log={log} />
            </Box>
            {
                0 < log.downstreams.length &&
                <Handle type="source" position={Position.Bottom} isConnectable={false} />
            }
        </>
    );
}

const nodeTypes = {
    planNode: PlanNode,
    inputNode: InputNode,
    outputNode: OutputNode,
    logNode: LogNode,
};

const PlanGraph: React.FC<{
    rootPlanId: string,
    planService: PlanService,
}> = ({ rootPlanId, planService }) => {
    type Link = {
        type: "input-to-plan" | "output-from-plan" | "output-to-input",
        source: string,
        target: string,
        weight?: number,
        style?: React.CSSProperties,
    }
    const [foundPlan, setFoundPlan] = React.useState<{ plan: PlanDetail, size?: { width: number, height: number } }[]>([]);
    const [foundInputs, setFoundInputs] = React.useState<{ planId: string, input: Input, size?: { width: number, height: number } }[]>([]);
    const [foundOutputs, setFoundOutputs] = React.useState<{ planId: string, output: Output, size?: { width: number, height: number } }[]>([]);
    const [foundLogs, setFoundLogs] = React.useState<{ planId: string, log: Log, size?: { width: number, height: number } }[]>([]);
    const [links, setLinks] = React.useState<Link[]>([]);
    const [selectedPlan, setSelectedPlan] = React.useState<PlanDetail | null>(null);
    const [selectedPlanIsExpandedd, setSelectedPlanIsExpanded] = React.useState<boolean>(false);


    const sameLink = (a: Link, b: Link) => {
        return a.type === b.type && a.source === b.source && a.target === b.target;
    };

    useEffect(() => {
        const fetchedPlan = [...foundPlan];
        const fetchedInputs = [...foundInputs];
        const fetchedOutputs = [...foundOutputs];
        const fetchedLogs = [...foundLogs];
        const fetchedLinks = [...links];

        const fetchPlan = async (planId: string) => {
            if (fetchedPlan.some((item) => item.plan.planId === planId)) { return; }

            const plan = await planService.fetchById(planId);
            fetchedPlan.push({ plan: plan });

            const inputs = plan.inputs.map((input) => {
                return { planId: plan.planId, input: input };
            });
            fetchedInputs.push(...inputs);

            const outputs = plan.outputs.map((output) => {
                return { planId: plan.planId, output: output };
            });
            fetchedOutputs.push(...outputs);

            if (plan.log) {
                fetchedLogs.push({ planId: plan.planId, log: plan.log });
            }

            const inputLinks: Link[] = plan.inputs.map((input) => {
                return {
                    type: "input-to-plan",
                    source: `${plan.planId}:${input.path}`,
                    target: plan.planId,
                    weight: 10,
                    style: { strokeWidth: 3 },
                };
            });
            const outputLinks: Link[] = plan.outputs.map((output) => {
                return {
                    type: "output-from-plan",
                    source: plan.planId,
                    target: `${plan.planId}:${output.path}`,
                    weight: 10,
                    style: { strokeWidth: 3 },
                };
            });
            const logLinks: Link[] = plan.log ? [{
                type: "output-from-plan",
                source: plan.planId,
                target: `${plan.planId}:log`,
                weight: 10,
                style: { strokeWidth: 3 },
            }] : [];

            fetchedLinks.push(...inputLinks, ...outputLinks, ...logLinks);

            for (const input of plan.inputs) {
                for (const upstream of input.upstreams) {
                    await fetchPlan(upstream.plan.planId);

                    let suffix: string;
                    if (upstream.mountpoint) {
                        suffix = upstream.mountpoint.path;
                    } else if (upstream.log) {
                        suffix = "log";
                    } else {
                        suffix = "";
                    }
                    const newLink: Link = {
                        type: "output-to-input",
                        source: `${upstream.plan.planId}:${suffix}`,
                        target: `${plan.planId}:${input.path}`,
                        weight: 1,
                    };

                    if (fetchedLinks.some((item) => sameLink(item, newLink))) { continue; }
                    fetchedLinks.push(newLink);
                }
            }
            for (const output of plan.outputs) {
                for (const downstream of output.downstreams) {
                    await fetchPlan(downstream.plan.planId);

                    const newLink: Link = {
                        type: "output-to-input",
                        source: `${plan.planId}:${output.path}`,
                        target: `${downstream.plan.planId}:${downstream.mountpoint.path}`,
                        weight: 1,
                    };

                    if (fetchedLinks.some((item) => sameLink(item, newLink))) { continue; }
                    fetchedLinks.push(newLink);
                }
            }
            if (plan.log) {
                for (const downstream of plan.log.downstreams) {
                    await fetchPlan(downstream.plan.planId);

                    const newLink: Link = {
                        type: "output-to-input",
                        source: `${plan.planId}:log`,
                        target: `${downstream.plan.planId}:${downstream.mountpoint.path}`,
                        weight: 1,
                    };

                    if (fetchedLinks.some((item) => sameLink(item, newLink))) { continue; }
                    fetchedLinks.push(newLink);
                }
            }
        };

        const fetchGraph = async () => {
            try {
                await fetchPlan(rootPlanId);
                setFoundPlan(fetchedPlan);
                setFoundInputs(fetchedInputs);
                setFoundOutputs(fetchedOutputs);
                setFoundLogs(fetchedLogs);
                setLinks(fetchedLinks);
            } catch (e) {
                console.error("Error fetching plan graph:", e);
            }
        };

        fetchGraph();
    }, [rootPlanId, planService]);

    type NodeParams = (
        | { type: "planNode", data: PlanNodeValues }
        | { type: "inputNode", data: InputNodeaValues }
        | { type: "outputNode", data: OutputNodeValues }
        | { type: "logNode", data: LogNodeValues }
    )
    const nodes: (NodeToBeLayouted & NodeParams)[] = [];

    for (const { plan, size } of foundPlan) {
        nodes.push({
            id: plan.planId,
            type: "planNode",
            size: size,
            data: {
                plan: plan,
                onResize: (planId: string, size: { width: number, height: number }) => {
                    setFoundPlan((prev) => {
                        const index = prev.findIndex((item) => item.plan.planId === planId);
                        if (index < 0) { return prev; }
                        const item = prev[index];
                        if (item.size?.width === size.width && item.size?.height === size.height) {
                            return prev;
                        }

                        const newPlans = [...prev];
                        newPlans[index] = { plan: item.plan, size: size };
                        return newPlans;
                    })
                },
                onClick: (plan: PlanDetail) => {
                    setSelectedPlan(plan);
                    setSelectedPlanIsExpanded(true);
                }
            }
        });
    }

    for (const { planId, input, size } of foundInputs) {
        nodes.push({
            id: `${planId}:${input.path}`,
            type: "inputNode",
            size: size,
            data: {
                planId: planId,
                input: input,
                onResize: (planId: string, path: string, size: { width: number, height: number }) => {
                    setFoundInputs((prev) => {
                        const index = prev.findIndex((item) => item.planId === planId && item.input.path === path);
                        if (index < 0) { return prev; }
                        const item = prev[index];
                        if (item.size?.width === size.width && item.size?.height === size.height) {
                            return prev;
                        }

                        const newInputs = [...prev];
                        newInputs[index] = { planId: item.planId, input: item.input, size: size };
                        return newInputs;
                    })
                }
            }
        });
    }

    for (const { planId, output, size } of foundOutputs) {
        nodes.push({
            id: `${planId}:${output.path}`,
            type: "outputNode",
            size: size,
            data: {
                planId: planId,
                output: output,
                onResize: (planId: string, path: string, size: { width: number, height: number }) => {
                    setFoundOutputs((prev) => {
                        const index = prev.findIndex((item) => item.planId === planId && item.output.path === path);
                        if (index < 0) { return prev; }
                        const item = prev[index];
                        if (item.size?.width === size.width && item.size?.height === size.height) {
                            return prev;
                        }

                        const newOutputs = [...prev];
                        newOutputs[index] = { planId: item.planId, output: item.output, size: size };
                        return newOutputs;
                    })
                }
            }
        });
    }

    for (const { planId, log, size } of foundLogs) {
        nodes.push({
            id: `${planId}:log`,
            type: "logNode",
            size: size,
            data: {
                planId: planId,
                log: log,
                onResize: (planId: string, size: { width: number, height: number }) => {
                    setFoundLogs((prev) => {
                        const index = prev.findIndex((item) => item.planId === planId);
                        if (index < 0) { return prev; }
                        const item = prev[index];
                        if (item.size?.width === size.width && item.size?.height === size.height) {
                            return prev;
                        }

                        const newLogs = [...prev];
                        newLogs[index] = { planId: item.planId, log: item.log, size: size };
                        return newLogs;
                    })
                }
            }
        });
    }

    const layoutedNodes = getLayoutedNodes(nodes, links);
    const edges = links.map((link) => {
        return {
            id: `${link.source}-${link.target}`,
            animated: true,
            ...link
        }
    });

    return (
        <Stack height="100%" direction="row" overflow="hidden">
            <ReactFlowProvider>
                <ReactFlow
                    nodes={layoutedNodes}
                    edges={edges}
                    nodeTypes={nodeTypes}
                    fitView
                    onClick={() => {
                        setSelectedPlan(null);
                        setSelectedPlanIsExpanded(false);
                    }}
                >
                    <Background color="#aaa" gap={16} />
                    <Controls />
                </ReactFlow>
            </ReactFlowProvider>
            <Collapse
                in={selectedPlan !== null}
                orientation="horizontal"
                sx={{ width: selectedPlan !== null ? "25vw" : undefined }}
            >
                <Box overflow="auto" height="100%">
                    {
                        selectedPlan &&
                        <PlanItem
                            plan={selectedPlan}
                            expanded={selectedPlanIsExpandedd}
                            setExpanded={(_, mode) => { setSelectedPlanIsExpanded(mode) }}
                        />
                    }
                </Box>
            </Collapse>
        </Stack>
    );
};

export default PlanGraph;
