import React, { useEffect } from "react";
import { useParams } from "react-router-dom";

const LineageGraph: React.FC = () => {
    const { id } = useParams<{ id: string }>();

    useEffect(() => {
        // リネージグラフの描画処理（D3.js を利用）
        console.log(`Rendering lineage graph for Data ID: ${id}`);
    }, [id]);

    return <div id="lineage-graph">Lineage Graph for Data ID: {id}</div>;
};

export default LineageGraph;
