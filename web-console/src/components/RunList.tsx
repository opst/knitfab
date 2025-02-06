import AutoModeIcon from "@mui/icons-material/AutoMode";
import CheckboxIcon from "@mui/icons-material/CheckBox";
import CheckboxBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import ClearIcon from "@mui/icons-material/Clear";
import ConstructionIcon from '@mui/icons-material/Construction';
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import InputIcon from '@mui/icons-material/Input';
import OutputIcon from '@mui/icons-material/Output';
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import RefreshIcon from "@mui/icons-material/Refresh";
import TodayIcon from "@mui/icons-material/Today";
import Alert from "@mui/material/Alert";
import Badge from "@mui/material/Badge";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import ButtonGroup from "@mui/material/ButtonGroup";
import Chip from "@mui/material/Chip";
import Collapse from "@mui/material/Collapse";
import Divider from "@mui/material/Divider";
import Grid from "@mui/material/Grid2";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { DateTimeField } from "@mui/x-date-pickers/DateTimeField";
import { DateTime } from "luxon";
import React, { useCallback, useEffect, useState } from "react";
import { RunService } from "../api/services/runService";
import { Duration } from "../api/services/types/time";
import { RunStatus, RunStatuses } from "../api/services/types/types";
import { RunDetail } from "../types/types";
import { DurationFilter } from "./Filter";
import { RunItem } from "./Items";

export type RunListProps = {
    runService: RunService;
};

const RunList: React.FC<RunListProps> = ({ runService }) => {
    const [runList, setRunList] = useState<RunDetail[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);
    const [autoRefresh, setAutoRefresh] = useState<boolean>(false);
    const [expanded, setExpanded] = useState<Set<string>>(new Set());
    const [logExpanded, setLogExpanded] = useState<Set<string>>(new Set());
    const [filter, setFilter] = useState<RunFilterParams>({
        planId: [],
        knitIdInput: [],
        knitIdOutput: [],
        status: [],
    });
    const [filterIsVisible, setFilterIsVisible] = useState<boolean>(false);

    const updateExpanded = useCallback((planId: string, mode: boolean) => {
        if (mode) {
            setExpanded((prev) => {
                const next = new Set(prev);
                next.add(planId);
                return next;
            });
        } else {
            setExpanded((prev) => {
                const next = new Set(prev);
                next.delete(planId);
                return next;
            });
        }
    }, [setExpanded])

    const updateLogExpanded = useCallback((planId: string, mode: boolean) => {
        if (mode) {
            setLogExpanded((prev) => {
                const next = new Set(prev);
                next.add(planId);
                return next;
            });
        } else {
            setLogExpanded((prev) => {
                const next = new Set(prev);
                next.delete(planId);
                return next;
            });
        }
    }, [setLogExpanded])

    const fetchData = async () => {
        setLoading(true);
        try {
            const response = await runService.fetchList(filter);
            setRunList(response);
            setError(null);
        } catch (err) {
            console.error("Error fetching data:", err);
            setError("Error fetching data");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, [runService, filter]);

    useEffect(() => {
        if (autoRefresh) {
            fetchData();
            const interval = setInterval(fetchData, 5000);
            return () => clearInterval(interval);
        }
    }, [autoRefresh, filter]);

    return (
        <Stack spacing={2}>
            <Stack spacing={2} direction="row">
                <Badge
                    invisible={
                        filter.planId.length === 0 &&
                        filter.knitIdInput.length === 0 &&
                        filter.knitIdOutput.length === 0 &&
                        !filter.since &&
                        !filter.duration?.hours !== undefined &&
                        !filter.duration?.minutes !== undefined &&
                        !filter.duration?.seconds !== undefined
                    }
                    color="primary">
                    <Button
                        onClick={() => setFilterIsVisible(!filterIsVisible)}
                        startIcon={filterIsVisible ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    >
                        {filterIsVisible ? "Hide Filters" : "Show Filters"}
                    </Button>
                </Badge>
                <Box flexGrow={1}>
                    <Collapse in={filterIsVisible}>
                        <RunFilter values={filter} onChange={setFilter} />
                    </Collapse>
                </Box>
                <Divider orientation="vertical" flexItem />
                <Box alignContent="center">
                    <ButtonGroup>
                        <Button
                            onClick={fetchData}
                            disabled={loading}
                            startIcon={
                                autoRefresh ? <AutoModeIcon /> : <RefreshIcon />
                            }
                            variant="contained"
                        >
                            {loading ? "Loading..." : "Refresh"}
                        </Button>
                        <Divider orientation="vertical" flexItem />
                        <Button
                            onClick={() => setAutoRefresh(!autoRefresh)}
                            variant="contained"
                            startIcon={
                                autoRefresh ? <CheckboxIcon /> : <CheckboxBlankIcon />
                            }
                        >
                            Auto(5s)
                        </Button>
                    </ButtonGroup>
                </Box>
            </Stack>
            {error && <Alert severity="error">{error}</Alert>}
            {
                !error && (
                    <Stack key="item-list">
                        {runList.map((run) => (
                            <RunItem
                                key={run.runId}
                                run={run}
                                expanded={expanded.has(run.runId)}
                                setExpanded={updateExpanded}
                                logExpanded={logExpanded.has(run.runId)}
                                setLogExpanded={updateLogExpanded}
                                runService={runService}
                            />
                        ))}
                    </Stack>
                )
            }
        </Stack >
    );
};

type RunFilterParams = {
    planId: string[];
    knitIdInput: string[];
    knitIdOutput: string[];
    status: RunStatus[];
    since?: DateTime;
    duration?: Duration;
};


export type RunFilterProps = {
    values: RunFilterParams;
    onChange: (values: RunFilterParams) => void;
};


const RunFilter: React.FC<RunFilterProps> = ({ values, onChange }) => {
    const [planIdInput, setPlanIdInput] = useState("");
    const [knitIdInput, setKnitIdInput] = useState("");
    const [knitIdOutput, setKnitIdOutput] = useState("");

    const addPlanId = () => {
        if (planIdInput.trim() && !values.planId.includes(planIdInput)) {
            onChange({ ...values, planId: [...values.planId, planIdInput] });
            setPlanIdInput("");
        }
    };

    const addKnitIdInput = () => {
        if (knitIdInput.trim() && !values.knitIdInput.includes(knitIdInput)) {
            onChange({ ...values, knitIdInput: [...values.knitIdInput, knitIdInput] });
            setKnitIdInput("");
        }
    };

    const addKnitIdOutput = () => {
        if (knitIdOutput.trim() && !values.knitIdOutput.includes(knitIdOutput)) {
            onChange({ ...values, knitIdOutput: [...values.knitIdOutput, knitIdOutput] });
            setKnitIdOutput("");
        }
    };

    const setDuration = (duration: Duration | null) => {
        if (!duration) {
            duration = {};
        }
        onChange({ ...values, duration });
    };

    return (
        <Stack spacing={2}>
            <Grid container direction="row" spacing={1} alignItems="center">
                <Grid>
                    <Chip icon={<ConstructionIcon />} label="Plan IDs" />
                </Grid>
                {values.planId.map(id => (
                    <Grid>
                        <Chip key={id} label={id} onDelete={() => onChange({ ...values, planId: values.planId.filter(p => p !== id) })} />
                    </Grid>
                ))}
                <Grid flexGrow={1} minWidth="50%">
                    <Stack direction="row" spacing={1} alignItems="center">
                        <TextField
                            label="Add Plan ID"
                            variant="filled"
                            value={planIdInput}
                            onChange={(e) => setPlanIdInput(e.target.value)}
                            fullWidth
                        />
                        <Button variant="contained" onClick={addPlanId}>Add</Button>
                    </Stack>
                </Grid>
            </Grid>
            <Grid container direction="row" spacing={1} alignItems="center">
                <Grid>
                    <Chip icon={<InputIcon />} label="Input Knit IDs" />
                </Grid>
                {values.knitIdInput.map(id => (
                    <Grid>
                        <Chip key={id} label={id} onDelete={() => onChange({ ...values, knitIdInput: values.knitIdInput.filter(k => k !== id) })} />
                    </Grid>
                ))}
                <Grid flexGrow={1} minWidth="50%">
                    <Stack direction="row" spacing={1} alignItems="center">
                        <TextField
                            label="Add Input Knit ID"
                            variant="filled"
                            value={knitIdInput}
                            onChange={(e) => setKnitIdInput(e.target.value)}
                            fullWidth
                        />
                        <Button variant="contained" onClick={addKnitIdInput}>Add</Button>
                    </Stack>
                </Grid>
            </Grid>
            <Grid container direction="row" spacing={1} alignItems="center">
                <Grid>
                    <Chip icon={<OutputIcon />} label="Output Knit IDs" />
                </Grid>
                {values.knitIdOutput.map(id => (
                    <Grid>
                        <Chip key={id} label={id} onDelete={() => onChange({ ...values, knitIdOutput: values.knitIdOutput.filter(k => k !== id) })} />
                    </Grid>
                ))}
                <Grid flexGrow={1} minWidth="50%">
                    <Stack direction="row" spacing={1} alignItems="center">
                        <TextField
                            label="Add Output Knit ID"
                            variant="filled"
                            value={knitIdOutput}
                            onChange={(e) => setKnitIdOutput(e.target.value)}
                            fullWidth
                        />
                        <Button variant="contained" onClick={addKnitIdOutput}>Add</Button>
                    </Stack>
                </Grid>
            </Grid>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip icon={<PlayArrowIcon />} label="Run Status" />
                <Select
                    multiple
                    value={values.status}
                    onChange={(e) => onChange({ ...values, status: e.target.value as RunFilterProps["values"]["status"] })}
                    displayEmpty
                    fullWidth
                    variant="filled"
                >
                    {RunStatuses.map(status => (
                        <MenuItem key={status} value={status}>{status}</MenuItem>
                    ))}
                </Select>
            </Stack>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip icon={<TodayIcon />} label="Timestamp" />
                <DateTimeField
                    label="Since"
                    value={values.since || null}
                    onChange={(newValue) => onChange({ ...values, since: newValue || undefined })}
                    variant="filled"
                    fullWidth
                />
            </Stack>
            <Stack direction="row" spacing={1} alignItems="center">
                <Typography alignContent="center">to</Typography>
                <DurationFilter
                    value={values.duration ?? {}}
                    onChange={setDuration}
                    disabled={!(values.since?.isValid)}
                    variant="filled"
                />
                <Typography alignContent="center">later</Typography>
            </Stack>
            <Box>
                <Button
                    variant="contained"
                    startIcon={<ClearIcon />}
                    onClick={() => onChange({
                        planId: [],
                        knitIdInput: [],
                        knitIdOutput: [],
                        status: [],
                        since: undefined,
                        duration: undefined
                    })}
                >
                    Clear
                </Button>
            </Box>
        </Stack>
    );
};

export default RunList;
