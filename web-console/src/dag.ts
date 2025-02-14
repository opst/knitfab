import dagre from "@dagrejs/dagre";


const nodeWidth = 500;
const nodeHeight = 500;

export type NodeToBeLayouted = { id: string, size?: { width: number, height: number } };

export type NodeLayouted = NodeToBeLayouted & { position: { x: number, y: number } };

export type EdgeDescriptor = {
    source: string,
    target: string,
    weight?: number
};

export const getLayoutedNodes = <T>(
    nodes: (NodeToBeLayouted & T)[],
    edges: EdgeDescriptor[],
): (NodeLayouted & T)[] => {
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
        dagreGraph.setEdge(edge.source, edge.target, { weight: edge.weight });
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