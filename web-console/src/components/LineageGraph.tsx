import React, { useEffect, useRef, useState } from "react";
import {
    ReactFlow,
    ReactFlowProvider,
    Background,
    Controls,
    Node,
    Edge,
    Handle,
    Position,
    NodeProps,
} from "@xyflow/react"
import dagre from "@dagrejs/dagre";
import "@xyflow/react/dist/style.css";
import { DataService } from "../api/services/dataService";
import { RunService } from "../api/services/runService";
import { DataCard, RunCard } from "./Items";
import Stack from "@mui/material/Stack";
import Box from "@mui/material/Box";
import { DataDetail, RunDetail } from "../types/types";

type DataNodeProps = NodeProps<
    Node<
        {
            data: DataDetail,
            onResize: (knitId: string, size: { width: number, height: number }) => void,
        },
        "dataNode"
    >
>

const DataNode: React.FC<DataNodeProps> = ({
    data,
}) => {
    const ref = useRef<HTMLElement>(null);
    useEffect(() => {
        const observer = new ResizeObserver((entries) => {
            entries.forEach((entry) => {
                const node = entry.target;
                data.onResize(data.data.knitId, { width: node.clientWidth, height: node.clientHeight })
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
                <DataCard data={data.data} />
            </Box>
            {
                0 < data.data.downstreams.length &&
                <Handle type="source" position={Position.Bottom} isConnectable={false} />
            }
        </>
    )
};

type RunNodeProps = NodeProps<
    Node<
        {
            run: RunDetail,
            onResize: (runId: string, size: { width: number, height: number }) => void,
        },
        "runNode"
    >
>

const RunNode: React.FC<RunNodeProps> = ({ data }) => {
    const ref = useRef<HTMLElement>(null);
    useEffect(() => {
        const observer = new ResizeObserver((entries) => {
            entries.forEach((entry) => {
                const node = entry.target;
                data.onResize(data.run.runId, { width: node.clientWidth, height: node.clientHeight })
            })
        })
        if (!ref.current) { return; }

        observer.observe(ref.current, { box: "border-box" });

        return () => { observer.disconnect(); }
    })
    return (
        <>
            {
                0 < data.run.inputs.length &&
                <Handle type="target" position={Position.Top} isConnectable={false} />
            }
            <Box maxWidth="33vw" ref={ref}>
                <RunCard run={data.run} />
            </Box>
            {
                0 < data.run.outputs.length &&
                <Handle type="source" position={Position.Bottom} isConnectable={false} />
            }
        </>
    )
};

const nodeTypes = {
    dataNode: DataNode,
    runNode: RunNode,
};

const nodeWidth = 500;
const nodeHeight = 500;

type NodeToBeLayouted = ({ id: string, size?: { width: number, height: number } }
    & (
        {
            type: "runNode",
            data: {
                run: RunDetail,
                onResize: (runId: string, size: { width: number, height: number }) => void,
            }
        }
        | {
            type: "dataNode",
            data: {
                data: DataDetail,
                onResize: (knitId: string, size: { width: number, height: number }) => void,
            }
        }
    )
);
type NodeLayouted = NodeToBeLayouted & { position: { x: number, y: number } };

const getLayoutedNodes = (
    nodes: NodeToBeLayouted[],
    edges: { source: string, target: string }[],
): NodeLayouted[] => {
    const minHeight = Math.min(nodeHeight, ...nodes.map((node) => node.size?.height ?? nodeHeight));

    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    dagreGraph.setGraph({
        rankdir: "TB",
        ranksep: minHeight / 2,
    });
    nodes.forEach((node) => {
        dagreGraph.setNode(node.id, { width: node.size?.width ?? nodeWidth, height: node.size?.height ?? nodeHeight });
    });
    edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target);
    });
    dagre.layout(dagreGraph);

    return nodes.map((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        return {
            ...node,
            position: {
                x: nodeWithPosition.x - nodeWithPosition.width / 2,
                y: nodeWithPosition.y - nodeWithPosition.height / 2,
            },
        };
    });
};

const LineageGraph = ({ dataService, runService, rootDataId, rootRunId }: { dataService: DataService; runService: RunService; rootDataId?: string; rootRunId?: string }) => {
    const [foundData, setFoundData] = useState<{ data: DataDetail, size?: { width: number, height: number } }[]>([]);
    const [foundRun, setFoundRun] = useState<{ run: RunDetail, size?: { width: number, height: number } }[]>([]);

    type Link = { type: "input" | "output", source: string, target: string, label: string };
    const [links, setLinks] = useState<Link[]>([]);
    const sameLink = (a: Link, b: Link) => {
        return a.source === b.source && a.target === b.target;
    }

    useEffect(() => {
        const fetchedData: typeof foundData = [...foundData];
        const fetchedRun: typeof foundRun = [...foundRun];
        const fetchedLinks: typeof links = [...links];

        const fetchData = async (knitId: string) => {
            if (fetchedData.find((n) => n.data.knitId === knitId)) { return; }
            const data = await dataService.fetchById(knitId);
            fetchedData.push({ data });
            if (data.upstream) {
                await fetchRun(data.upstream.run.runId);
            }
            for (const downstream of data.downstreams) {
                await fetchRun(downstream.run.runId);
            }
        };

        const fetchRun = async (runId: string) => {
            if (fetchedRun.find((n) => n.run.runId === runId)) { return; }
            const run = await runService.fetchById(runId);
            fetchedRun.push({ run });

            for (const input of run.inputs) {
                await fetchData(input.knitId);
                const newLink = {
                    type: "input" as const,
                    source: input.knitId,
                    target: run.runId,
                    label: input.path,
                };
                if (!links.find((e) => sameLink(e, newLink))) {
                    fetchedLinks.push(newLink);
                }
            }
            for (const output of run.outputs) {
                await fetchData(output.knitId);
                const newLink = {
                    type: "output" as const,
                    source: run.runId,
                    target: output.knitId,
                    label: output.path,
                };
                if (!links.find((e) => sameLink(e, newLink))) {
                    fetchedLinks.push(newLink);
                }
            }
            if (run.log) {
                await fetchData(run.log.knitId);
                const newLink = {
                    type: "output" as const,
                    source: run.runId,
                    target: run.log.knitId,
                    label: "(log)",
                };
                if (!links.find((e) => sameLink(e, newLink))) {
                    fetchedLinks.push(newLink);
                }
            }
        };

        const fetchGraph = async () => {
            try {
                if (rootDataId) {
                    await fetchData(rootDataId);
                }
                if (rootRunId) {
                    await fetchRun(rootRunId);
                }
                setFoundData(fetchedData);
                setFoundRun(fetchedRun);
                setLinks(fetchedLinks);
            } catch (error) {
                console.error("Error fetching lineage graph:", error);
            }
        };

        fetchGraph();
    }, [rootDataId, dataService, runService]);

    const edges = links.map((link) => {
        switch (link.type) {
            case "input":
                return {
                    id: `data-${link.source}/run-${link.target}`,
                    source: `data-${link.source}`,
                    target: `run-${link.target}`,
                    animated: true,
                    label: link.label,
                };
            case "output":
                return {
                    id: `run-${link.source}/data-${link.target}`,
                    source: `run-${link.source}`,
                    target: `data-${link.target}`,
                    animated: true,
                    label: link.label,
                };
        }
    });

    const layoutedNodes = getLayoutedNodes(
        [
            ...(foundData.map((data) => ({
                id: `data-${data.data.knitId}`,
                type: "dataNode" as const,
                size: data.size,
                data: {
                    data: data.data,
                    onResize: (knitId: string, size: { width: number, height: number }) => {
                        setFoundData((prev) => {
                            const index = prev.findIndex((n) => n.data.knitId === knitId);
                            if (index < 0) { return prev; }
                            const data = prev[index];
                            if (data.size?.width === size.width && data.size?.height === size.height) { return prev; }

                            const newData = [...prev];
                            newData[index] = { ...data, size, };
                            return newData;
                        })
                    },
                },
            }))),
            ...(foundRun.map((run) => ({
                id: `run-${run.run.runId}`,
                type: "runNode" as const,
                size: run.size,
                data: {
                    run: run.run,
                    onResize: (runId: string, size: { width: number, height: number }) => {
                        setFoundRun((prev) => {
                            const index = prev.findIndex((n) => n.run.runId === runId);
                            if (index < 0) { return prev; }
                            const run = prev[index];
                            if (run.size?.width === size.width && run.size?.height === size.height) { return prev; }

                            const newRun = [...prev];
                            newRun[index] = { ...run, size, };
                            return newRun;
                        })
                    },
                },
            }))),
        ],
        edges,
    );

    return (
        <Stack height="100%" >
            <ReactFlowProvider>
                <ReactFlow nodes={layoutedNodes} edges={edges} nodeTypes={nodeTypes} fitView>
                    <Background color="#aaa" gap={16} />
                    <Controls />
                </ReactFlow>
            </ReactFlowProvider>
        </Stack>
    );
};

export default LineageGraph;
