import React, { useCallback } from "react";
import { HashRouter as Router, Routes, Route, useLocation, Navigate, useNavigate } from "react-router-dom";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import DataList from "./components/DataList";
import PlanList from "./components/PlanList";
import RunList from "./components/RunList";
import { ApiClient } from "./api/apiClient";
import { DataService } from "./api/services/dataService";
import { PlanService } from "./api/services/planService";
import { RunService } from "./api/services/runService";
import { LocalizationProvider } from "@mui/x-date-pickers";
import { AdapterLuxon } from '@mui/x-date-pickers/AdapterLuxon';


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

        return (
            <Container>
                <Box sx={{ borderBottom: 1, borderColor: 'divider', marginBottom: 2 }}>
                    <Tabs value={getTabIndex()} aria-label="app navigation" textColor="primary" indicatorColor="primary">
                        <Tab label="Data" onClick={gotoData} />
                        <Tab label="Plans" onClick={gotoPlans} />
                        <Tab label="Runs" onClick={gotoRuns} />
                    </Tabs>
                </Box>
                <Routes>
                    <Route path="/data" element={<DataList dataService={dataService} />} />
                    <Route path="/plans" element={<PlanList planService={planService} />} />
                    <Route path="/runs" element={<RunList runService={runService} />} />
                    <Route path="/" element={<Navigate to="/data" />} />
                </Routes>
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