import CloseIcon from "@mui/icons-material/Close";
import Backdrop from "@mui/material/Backdrop";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Container from "@mui/material/Container";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import { LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterLuxon } from '@mui/x-date-pickers/AdapterLuxon';
import React, { useCallback, useEffect, useState } from "react";
import { Navigate, Route, HashRouter as Router, Routes, useLocation, useNavigate } from "react-router-dom";
import { ApiClient } from "./api/apiClient";
import { DataService } from "./api/services/dataService";
import { PlanService } from "./api/services/planService";
import { RunService } from "./api/services/runService";
import DataList from "./components/DataList";
import LineageGraph from "./components/LineageGraph";
import PlanList from "./components/PlanList";
import RunList from "./components/RunList";
import PlanGraph from "./components/PlanGraph";


const AppTabs: React.FC<{
    dataService: DataService,
    planService: PlanService,
    runService: RunService
}> = ({
    dataService,
    planService,
    runService
}) => {
        const location = useLocation();
        const navigate = useNavigate();

        const [lineageGraphRoot, setLineageGraphRoot] = useState<{ type: "run" | "data", id: string } | null>(null);
        const [plangraphRoot, setPlanGraphRoot] = useState<string | null>(null);

        const getTabIndex = () => {
            switch (location.pathname) {
                case "/plans":
                    return 1;
                case "/runs":
                    return 2;
                case "/data":
                default:
                    return 0;
            }
        };

        const gotoData = useCallback(() => {
            navigate("/data");
        }, [navigate])
        const gotoPlans = useCallback(() => {
            navigate("/plans");
        }, [navigate])
        const gotoRuns = useCallback(() => {
            navigate("/runs");
        }, [navigate])

        useEffect(() => {
            if (!lineageGraphRoot) {
                return;
            }

            const onEsc = (ev: KeyboardEvent) => {
                if (ev.key === "Escape") {
                    setLineageGraphRoot(null);
                }
            }

            document.body.addEventListener("keydown", onEsc);
            return () => {
                document.body.removeEventListener("keydown", onEsc);
            }
        }, [lineageGraphRoot])

        return (
            <Container>
                <Box sx={{ borderBottom: 1, borderColor: 'divider', marginBottom: 2 }}>
                    <Tabs value={getTabIndex()} aria-label="app navigation" textColor="primary" indicatorColor="primary">
                        <Tab label="Data" onClick={gotoData} />
                        <Tab label="Plans" onClick={gotoPlans} />
                        <Tab label="Runs" onClick={gotoRuns} />
                    </Tabs>
                </Box>
                <Stack direction="column" spacing={2}>
                    <Routes>
                        <Route
                            path="/data"
                            element={
                                <DataList
                                    dataService={dataService}
                                    setLineageGraphRoot={(knitId) => {
                                        setLineageGraphRoot({ type: "data", id: knitId });
                                        setPlanGraphRoot(null);
                                    }}
                                />
                            }
                        />
                        <Route
                            path="/plans"
                            element={
                                <PlanList
                                    planService={planService}
                                    setPlanGraphRoot={(planId) => {
                                        setPlanGraphRoot(planId);
                                        setLineageGraphRoot(null);
                                    }}
                                />
                            }
                        />
                        <Route
                            path="/runs"
                            element={
                                <RunList
                                    runService={runService}
                                    setLineageGraphRoot={(runId) => {
                                        setLineageGraphRoot({ type: "run", id: runId });
                                        setPlanGraphRoot(null);
                                    }}
                                />
                            }
                        />
                        <Route path="/" element={<Navigate to="/data" />} />
                    </Routes>
                </Stack>
                {
                    lineageGraphRoot &&
                    <Backdrop open={true}>
                        <Paper sx={{ width: "80vw", height: "80vh", overflow: "hidden" }}>
                            <Stack direction="column" sx={{ height: "100%" }}>
                                <Box>
                                    <Button startIcon={<CloseIcon />} onClick={() => setLineageGraphRoot(null)}>Close</Button>
                                </Box>
                                <Box flexGrow={1} position="relative" overflow="auto">
                                    <LineageGraph
                                        dataService={dataService}
                                        runService={runService}
                                        rootDataId={lineageGraphRoot.type === "data" ? lineageGraphRoot.id : undefined}
                                        rootRunId={lineageGraphRoot.type === "run" ? lineageGraphRoot.id : undefined}
                                    />
                                </Box>
                            </Stack>
                        </Paper>
                    </Backdrop>
                }
                {
                    plangraphRoot &&
                    <Backdrop open={true}>
                        <Paper sx={{ width: "80vw", height: "80vh", overflow: "hidden" }}>
                            <Stack direction="column" sx={{ height: "100%" }}>
                                <Box>
                                    <Button startIcon={<CloseIcon />} onClick={() => setPlanGraphRoot(null)}>Close</Button>
                                </Box>
                                <Box flexGrow={1} position="relative" overflow="auto">
                                    <PlanGraph
                                        planService={planService}
                                        rootPlanId={plangraphRoot}
                                    />
                                </Box>
                            </Stack>
                        </Paper>
                    </Backdrop>
                }
            </Container>
        );
    };

const App: React.FC<{ apiRoot: string }> = ({ apiRoot }) => {
    const client = new ApiClient(apiRoot);
    const dataService = new DataService(client);
    const planService = new PlanService(client);
    const runService = new RunService(client);
    return (
        <LocalizationProvider dateAdapter={AdapterLuxon}>
            <Router>
                <AppTabs
                    dataService={dataService}
                    planService={planService}
                    runService={runService}
                />
            </Router>
        </LocalizationProvider>
    );
};

export default App;