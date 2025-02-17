import dagre from "@dagrejs/dagre";


const nodeWidth = 50;
const nodeHeight = 50;

export type NodeLayouted = { position: { x: number, y: number } };

export type EdgeDescriptor = {
    source: string,
    target: string,
    weight?: number
};

export const getLayoutedNodes = <T extends { id: string }>(
    edges: EdgeDescriptor[],
    nodes: T[],
    getDimension: (node: T) => { width?: number, height?: number },
): (NodeLayouted & T)[] => {
    const dims = nodes.map((n) => ({
        id: n.id,
        ...getDimension(n),
    }));


    let minHeight = nodeHeight
    if (0 < dims.length) {
        minHeight = Math.min(...dims.map((node) => node.height ?? nodeHeight));
    }

    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    dagreGraph.setGraph({
        rankdir: "TB",
        ranksep: minHeight / 2,
    });
    dims.forEach((node) => {
        dagreGraph.setNode(node.id, { width: node.width ?? nodeWidth, height: node.height ?? nodeHeight });
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