import { Collapse } from "@mui/material";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import {
    Background,
    Controls,
    Edge,
    Handle,
    Node,
    NodeProps,
    Position,
    ReactFlow,
    ReactFlowProvider,
    useEdgesState,
    useNodesState,
    useReactFlow,
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
    onClick: (data: DataDetail) => void,
};

const DataNode: React.FC<NodeProps<Node<DataNodeValues, "dataNode">>> = ({
    data,
}) => {
    return (
        <>
            <Handle type="target" position={Position.Top} isConnectable={false} />
            <Box maxWidth="33vw" onClick={(ev) => {
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
    onClick: (run: RunDetail) => void,
};

const RunNode: React.FC<NodeProps<Node<RunNodeValues, "runNode">>> = ({ data }) => {
    return (
        <>
            {
                0 < data.run.inputs.length &&
                <Handle type="target" position={Position.Top} isConnectable={false} />
            }
            <Box maxWidth="33vw" onClick={(ev) => {
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


const LineageGraphInner = ({ dataService, runService, rootDataId, rootRunId }: { dataService: DataService; runService: RunService; rootDataId?: string; rootRunId?: string }) => {

    type NodeVariants = (
        { type: "runNode", data: RunNodeValues, }
        | { type: "dataNode", data: DataNodeValues, }
    );

    const [nodes, setNodes, onNodesChanged] = useNodesState<Node & NodeVariants>([]);
    const [edges, setEdges, onEdgeChanged] = useEdgesState<Edge>([]);
    const [selectedData, setSelectedData] = useState<DataDetail | null>(null);
    const [selectedDataIsExpanded, setSelectedDataIsExpanded] = useState(false);
    const [selectedRun, setSelectedRun] = useState<RunDetail | null>(null);
    const [selectedRunIsExpanded, setSelectedRunIsExpanded] = useState(false);
    const [selectedRunLogIsExpanded, setSelectedRunLogIsExpanded] = useState(false);

    type Link = { type: "input" | "output", source: string, target: string, label: string };
    const sameLink = (a: Link, b: Link) => {
        return a.source === b.source && a.target === b.target;
    }

    useEffect(() => {
        const fetchedData: { data: DataDetail }[] = [];
        const fetchedRun: { run: RunDetail }[] = [];
        const fetchedLinks: Link[] = [];

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
                if (!fetchedLinks.find((e) => sameLink(e, newLink))) {
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
                if (!fetchedLinks.find((e) => sameLink(e, newLink))) {
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
                if (!fetchedLinks.find((e) => sameLink(e, newLink))) {
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

                const _edges = fetchedLinks.map((link) => {
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
                setEdges(_edges);

                const layoutedNodes = getLayoutedNodes(
                    _edges,
                    [
                        ...(fetchedData.map((data) => ({
                            id: `data-${data.data.knitId}`,
                            type: "dataNode" as const,
                            data: {
                                data: data.data,
                                onClick: (data: DataDetail) => {
                                    setSelectedData(data);
                                    setSelectedDataIsExpanded(false);
                                    setSelectedRun(null);
                                    setSelectedRunIsExpanded(false);
                                    setSelectedRunLogIsExpanded(false);
                                },
                            },
                        }))),
                        ...(fetchedRun.map((run) => ({
                            id: `run-${run.run.runId}`,
                            type: "runNode" as const,
                            data: {
                                run: run.run,
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
                    () => ({}), // default size
                );
                setNodes(layoutedNodes.map((node) => ({
                    draggable: false,
                    ...node,
                })));

            } catch (error) {
                console.error("Error fetching lineage graph:", error);
            }
        };

        fetchGraph();
    }, [rootDataId, dataService, runService]);

    const reactflow = useReactFlow();

    // this is need to avoid that selecting node invokes fitView
    const [fireFitView, setFireFitView] = React.useState({});
    useEffect(() => { reactflow.fitView(); }, [fireFitView])

    return (
        <>
            <ReactFlow
                nodes={nodes}
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
                onEdgesChange={onEdgeChanged}
                onNodesChange={(updatedNodes) => {
                    onNodesChanged(updatedNodes);
                    setNodes((prev) => {
                        const updated = updatedNodes.some((change) => (change.type === "dimensions"));
                        if (!updated) {
                            return prev
                        }
                        setFireFitView({});
                        return getLayoutedNodes(edges, prev, (node) => ({
                            width: node.measured?.width,
                            height: node.measured?.height,
                        }));
                    });
                }}
            >
                <Background color="#aaa" gap={16} />
                <Controls />
            </ReactFlow>
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
        </>
    );
};

const LineageGraph = ({ dataService, runService, rootDataId, rootRunId }: { dataService: DataService; runService: RunService; rootDataId?: string; rootRunId?: string }) => {
    return (
        <Stack height="100%" direction="row" overflow="hidden">
            <ReactFlowProvider>
                <LineageGraphInner dataService={dataService} runService={runService} rootDataId={rootDataId} rootRunId={rootRunId} />
            </ReactFlowProvider>
        </Stack>
    );
};

export default LineageGraph;
