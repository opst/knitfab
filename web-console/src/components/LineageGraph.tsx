import { Collapse } from "@mui/material";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
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
import "@xyflow/react/dist/style.css";
import React, { useEffect, useRef, useState } from "react";
import { DataService } from "../api/services/dataService";
import { RunService } from "../api/services/runService";
import { getLayoutedNodes } from "../dag";
import { DataDetail, RunDetail } from "../types/types";
import { DataCard, DataItem, RunCard, RunItem } from "./Items";

type DataNodeValues = {
    data: DataDetail,
    onResize: (knitId: string, size: { width: number, height: number }) => void,
    onClick: (data: DataDetail) => void,
};

const DataNode: React.FC<NodeProps<Node<DataNodeValues, "dataNode">>> = ({
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
            <Box maxWidth="33vw" ref={ref} onClick={(ev) => {
                ev.stopPropagation();
                data.onClick(data.data);
            }}>
                <DataCard data={data.data} />
            </Box>
            {
                0 < data.data.downstreams.length &&
                <Handle type="source" position={Position.Bottom} isConnectable={false} />
            }
        </>
    )
};

type RunNodeValues = {
    run: RunDetail,
    onResize: (runId: string, size: { width: number, height: number }) => void,
    onClick: (run: RunDetail) => void,
};

const RunNode: React.FC<NodeProps<Node<RunNodeValues, "runNode">>> = ({ data }) => {
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
            <Box maxWidth="33vw" ref={ref} onClick={(ev) => {
                ev.stopPropagation();
                data.onClick(data.run);
            }}>
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


const LineageGraph = ({ dataService, runService, rootDataId, rootRunId }: { dataService: DataService; runService: RunService; rootDataId?: string; rootRunId?: string }) => {
    const [foundData, setFoundData] = useState<{ data: DataDetail, size?: { width: number, height: number } }[]>([]);
    const [foundRun, setFoundRun] = useState<{ run: RunDetail, size?: { width: number, height: number } }[]>([]);
    const [selectedData, setSelectedData] = useState<DataDetail | null>(null);
    const [selectedDataIsExpanded, setSelectedDataIsExpanded] = useState(false);
    const [selectedRun, setSelectedRun] = useState<RunDetail | null>(null);
    const [selectedRunIsExpanded, setSelectedRunIsExpanded] = useState(false);
    const [selectedRunLogIsExpanded, setSelectedRunLogIsExpanded] = useState(false);

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

    type NodeParams = (
        { type: "runNode", data: RunNodeValues, }
        | { type: "dataNode", data: DataNodeValues, }
    );
    const layoutedNodes = getLayoutedNodes<NodeParams>(
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
                    onClick: (data: DataDetail) => {
                        setSelectedData(data);
                        setSelectedDataIsExpanded(false);
                        setSelectedRun(null);
                        setSelectedRunIsExpanded(false);
                        setSelectedRunLogIsExpanded(false);
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
                    onClick: (run: RunDetail) => {
                        setSelectedData(null);
                        setSelectedDataIsExpanded(false);
                        setSelectedRun(run);
                        setSelectedRunIsExpanded(false);
                        setSelectedRunLogIsExpanded(false);
                    },
                },
            }))),
        ],
        edges,
    );

    return (
        <Stack height="100%" direction="row" overflow="hidden">
            <ReactFlowProvider>
                <ReactFlow
                    nodes={layoutedNodes}
                    edges={edges}
                    nodeTypes={nodeTypes}
                    fitView
                    onClick={() => {
                        setSelectedData(null);
                        setSelectedDataIsExpanded(false);
                        setSelectedRun(null);
                        setSelectedRunIsExpanded(false);
                        setSelectedRunLogIsExpanded(false);
                    }}
                >
                    <Background color="#aaa" gap={16} />
                    <Controls />
                </ReactFlow>
            </ReactFlowProvider>
            <Collapse
                in={selectedData !== null || selectedRun !== null}
                orientation="horizontal"
                sx={{ width: selectedData !== null || selectedRun !== null ? "25vw" : undefined }}
            >
                <Box overflow="auto" height="100%">
                    {
                        selectedData &&
                        <DataItem
                            data={selectedData}
                            expanded={selectedDataIsExpanded}
                            setExpanded={(_, mode) => { setSelectedDataIsExpanded(mode) }}
                        />
                    }
                    {
                        selectedRun &&
                        <RunItem
                            run={selectedRun}
                            expanded={selectedRunIsExpanded}
                            setExpanded={(_, mode) => { setSelectedRunIsExpanded(mode) }}
                            logExpanded={selectedRunLogIsExpanded}
                            setLogExpanded={(_, mode) => { setSelectedRunLogIsExpanded(mode) }}
                            runService={runService}
                        />
                    }
                </Box>
            </Collapse>
        </Stack>
    );
};

export default LineageGraph;
