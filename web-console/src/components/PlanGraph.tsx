import React, { useEffect } from "react";
import { useParams } from "react-router-dom";

const DependencyGraph: React.FC = () => {
    const { id } = useParams<{ id: string }>();

    useEffect(() => {
        // 依存関係グラフの描画処理（D3.js を利用）
        console.log(`Rendering dependency graph for Plan ID: ${id}`);
    }, [id]);

    return <div id="dependency-graph">Dependency Graph for Plan ID: {id}</div>;
};

export default DependencyGraph;
