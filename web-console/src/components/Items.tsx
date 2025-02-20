import ConstructionIcon from '@mui/icons-material/Construction';
import DoneIcon from '@mui/icons-material/Done';
import ErrorIcon from '@mui/icons-material/Error';
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import InputIcon from '@mui/icons-material/Input';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import OutputIcon from '@mui/icons-material/Output';
import PendingIcon from '@mui/icons-material/Pending';
import PlayIcon from '@mui/icons-material/PlayArrow';
import StorageIcon from '@mui/icons-material/Storage';
import SubjectIcon from "@mui/icons-material/Subject";
import TagIcon from '@mui/icons-material/Tag';
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardActions from "@mui/material/CardActions";
import CardContent from "@mui/material/CardContent";
import CardHeader from "@mui/material/CardHeader";
import Chip from '@mui/material/Chip';
import Collapse from "@mui/material/Collapse";
import Grid2 from "@mui/material/Grid2";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableRow from "@mui/material/TableRow";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import React, { useEffect, useState } from "react";
import { RunService } from "../api/services/runService";
import { TagString } from "../api/services/types/types";
import { DataDetail, DataSummary, LogPoint, Mountpoint, PlanDetail, PlanSummary, RunDetail, RunSummary, Tag } from "../types/types";

/** Component to render a single Tag as a chip */
export const TagChip: React.FC<{ tag: Tag | TagString, onDelete?: (event: any) => void }> = ({ tag, onDelete }) => {
    if (typeof tag === "string") {
        const [key, value] = tag.split(/(?<=^[^:]*):/);
        return (
            <Chip
                label={`${key}: ${value}`}
                color="primary"
                variant="outlined"
                sx={{ margin: "4px" }}
                onDelete={onDelete}
            />
        );
    }

    const isTimestamp = tag.key === "knit#timestamp";
    const displayValue = isTimestamp
        ? new Date(tag.value).toLocaleString() // Convert RFC3339 to local time
        : tag.value;

    return (
        <Chip
            label={`${tag.key}: ${displayValue}`}
            color="primary"
            variant="outlined"
            sx={{ margin: "4px" }}
            onDelete={onDelete}
        />
    );
};

const TagSet = ({ tags }: { tags: Tag[] }) => {
    return (
        <Stack direction="row" flexWrap="wrap" sx={{ alignItems: 'center' }} spacing={1}>
            <Chip icon={<TagIcon />} label="tags" />
            {tags.length === 0
                ? <Typography fontStyle="italic">(No tags)</Typography>
                : tags.map((tag, index) => (
                    <TagChip key={index} tag={tag} />
                ))
            }
        </Stack>
    );
}

const DataCard = ({ data, variant = "outlined", elevation = 1, action, children }: {
    data: DataSummary,
    variant?: "outlined" | "elevation",
    elevation?: number
    action?: React.ReactNode,
    children?: React.ReactNode,
}) => {
    const allch = React.Children.toArray(children);

    const content: React.ReactNode[] = [];
    const actions: React.ReactNode[] = [];

    for (let c of allch) {
        if (React.isValidElement(c) && c.type === CardActions) {
            actions.push(c);
        } else {
            content.push(c);
        }
    }

    return (
        <Card
            variant={variant}
            elevation={elevation}
            sx={{ margin: "16px" }}
        >
            <CardHeader
                subheader="Data"
                avatar={<Tooltip title="Data"><StorageIcon /></Tooltip>}
                title={`Knit ID: ${data.knitId}`}
                action={action}
            />
            <CardContent>
                <TagSet tags={data.tags} />
                {content}
            </CardContent>
            {actions}
        </Card>
    )
}

/** Component to display DataDetail as a Card */
const DataItem: React.FC<{
    data: DataDetail,
    variant?: "outlined" | "elevation",
    elevation?: number,
    action?: React.ReactNode,
    expanded: boolean,
    setExpanded: (knitId: string, mode: boolean) => void
}> = ({ data, variant, elevation, action, expanded, setExpanded }) => {
    return (
        <DataCard variant={variant} elevation={elevation} data={data} action={action}>
            <Collapse in={expanded} timeout="auto" unmountOnExit>
                <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                    Upstream:
                </Typography>
                <Stack sx={{ marginLeft: "16px" }}>
                    {data.upstream.mountpoint && (
                        <RunCard run={data.upstream.run}>
                            <OutputPointCard mountpoint={data.upstream.mountpoint} />
                        </RunCard>
                    )}
                    {data.upstream.log && (
                        <RunCard run={data.upstream.run}>
                            <LogPointCard log={data.upstream.log} />
                        </RunCard>
                    )}
                </Stack>

                <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                    Downstreams:
                </Typography>
                {
                    data.downstreams.length === 0
                        ? <Typography fontStyle="italic">(No downstreams)</Typography>
                        : <Stack sx={{ marginLeft: "16px" }}>
                            {data.downstreams.map((assigned, index) => (
                                <RunCard run={assigned.run} key={index}>
                                    <InputPointCard mountpoint={assigned.mountpoint} />
                                </RunCard>
                            ))}
                        </Stack>
                }

                <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                    Nomination:
                </Typography>
                {
                    data.nomination.length === 0
                        ? <Typography fontStyle="italic">(No nominations)</Typography>
                        : <Stack sx={{ marginLeft: "16px" }}>
                            {data.nomination.map((nomination, index) => (
                                <PlanCard key={index} plan={nomination.plan} >
                                    <InputPointCard mountpoint={nomination} />
                                </PlanCard>
                            ))}
                        </Stack>
                }
            </Collapse>
            <CardActions>
                <Button
                    variant="contained"
                    color="primary"
                    startIcon={expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    onClick={() => { setExpanded(data.knitId, !expanded) }}
                    sx={{ marginTop: "16px" }}
                >
                    {expanded ? "Hide Details" : "Show Details"}
                </Button>
            </CardActions>
        </DataCard >
    );
};

const PlanCard = ({ plan, variant = "outlined", elevation = 1, action, children, subheader }: {
    plan: PlanSummary,
    variant?: "outlined" | "elevation",
    elevation?: number,
    action?: React.ReactNode,
    children?: React.ReactNode,
    subheader?: string,
}) => {
    const allch = React.Children.toArray(children);

    const content: React.ReactNode[] = [];
    const actions: React.ReactNode[] = [];

    for (let c of allch) {
        if (React.isValidElement(c) && c.type === CardActions) {
            actions.push(c);
        } else {
            content.push(c);
        }
    }

    return (
        <Card variant={variant} elevation={elevation} sx={{ margin: "16px" }}>
            <CardHeader
                avatar={<Tooltip title="Plan"><InsertDriveFileIcon /></Tooltip>}
                title={`Plan ID: ${plan.planId}`}
                subheader={subheader ? `Plan / ${subheader}` : "Plan"}
                action={action}
            />
            <CardContent>
                <TableContainer>
                    <Table>
                        <TableBody>
                            {
                                plan.image &&
                                <TableRow>
                                    <TableCell><Typography variant="subtitle1">Image</Typography></TableCell>
                                    <TableCell>{plan.image}</TableCell>
                                </TableRow>
                            }
                            {
                                plan.name &&
                                <TableRow>
                                    <TableCell><Typography variant="subtitle1">Name</Typography></TableCell>
                                    <TableCell>{plan.name}</TableCell>
                                </TableRow>
                            }
                            {
                                0 < plan.entrypoint.length &&
                                <TableRow>
                                    <TableCell><Typography variant="subtitle1">Entrypoint</Typography></TableCell>
                                    <TableCell><Typography variant="body1" fontFamily="monospace">{plan.entrypoint.join(" ")}</Typography></TableCell>
                                </TableRow>
                            }
                            {
                                0 < plan.args.length &&
                                <TableRow>
                                    <TableCell><Typography variant="subtitle1">Args</Typography></TableCell>
                                    <TableCell><Typography variant="body1" fontFamily="monospace">{plan.args.join(" ")}</Typography></TableCell>
                                </TableRow>
                            }
                            {
                                0 < plan.annotations.length &&
                                <TableRow>
                                    <TableCell><Typography variant="subtitle1">Annotations</Typography></TableCell>
                                    <TableCell>
                                        <Stack direction="row" flexWrap="wrap" spacing={2} >
                                            {plan.annotations.map((a, index) => (
                                                <Chip key={index} label={a} color="primary" variant="outlined" />
                                            ))}
                                        </Stack>
                                    </TableCell>
                                </TableRow>
                            }
                        </TableBody>
                    </Table>
                </TableContainer>
                {content}
                {actions}
            </CardContent>
        </Card >
    )
};

/** Card to display PlanSummary */
const PlanSummaryCard: React.FC<{
    plan: PlanSummary,
    variant?: "outlined" | "elevation",
    elevation?: number,
}> = ({ plan, variant, elevation }) => (
    <PlanCard plan={plan} variant={variant} elevation={elevation} />
);

/** Component to display PlanDetail as a Card */
const PlanItem: React.FC<{
    plan: PlanDetail,
    variant?: "outlined" | "elevation",
    elevation?: number,
    expanded: boolean,
    action?: React.ReactNode,
    setExpanded: (planId: string, mode: boolean) => void,
}> = ({ plan, variant = "outlined", expanded, action, setExpanded }) => {
    return (
        <PlanCard variant={variant} subheader={plan.active ? "active" : "deactivated"} plan={plan} action={action}>
            <Collapse in={expanded} timeout="auto" unmountOnExit>
                <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                    Inputs:
                </Typography>
                <Stack sx={{ marginLeft: "16px" }}>
                    {plan.inputs.map((input) => (
                        <InputPointCard key={input.path} mountpoint={input} />
                    ))}
                </Stack>
                <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                    Outputs:
                </Typography>
                <Stack sx={{ marginLeft: "16px" }}>
                    {plan.outputs.map((output) => (
                        <OutputPointCard key={output.path} mountpoint={output} />
                    ))}
                    {plan.log && (
                        <LogPointCard log={plan.log} />
                    )}
                </Stack>
                <Grid2 container direction="row" spacing={2}>
                    <Grid2 size="grow">
                        <Card variant="outlined">
                            <CardHeader title={<Typography variant="body2">Resources:</Typography>} />
                            <CardContent>
                                <Stack>
                                    {Object.entries(plan.resources).map(([key, value], index) => (
                                        <Typography key={index}>{`${key}=${value}`}</Typography>
                                    ))}
                                </Stack>
                            </CardContent>
                        </Card>
                    </Grid2>
                    {plan.onNode && (
                        <Grid2 size="grow">
                            <Card variant="outlined">
                                <CardHeader title={<Typography variant="body2">On Node:</Typography>} />
                                <CardContent>
                                    <TableContainer>
                                        <Table>
                                            <TableBody>
                                                <TableRow>
                                                    <TableCell><Typography variant="subtitle1">May</Typography></TableCell>
                                                    <TableCell>{plan.onNode.may.join(" ")}</TableCell>
                                                </TableRow>
                                                <TableRow>
                                                    <TableCell><Typography variant="subtitle1">Prefer</Typography></TableCell>
                                                    <TableCell>{plan.onNode.prefer.join(" ")}</TableCell>
                                                </TableRow>
                                                <TableRow>
                                                    <TableCell><Typography variant="subtitle1">Must</Typography></TableCell>
                                                    <TableCell>{plan.onNode.must.join(" ")}</TableCell>
                                                </TableRow>
                                            </TableBody>
                                        </Table>
                                    </TableContainer>
                                </CardContent>
                            </Card>
                        </Grid2>
                    )}
                    {plan.serviceAccount && (
                        <Grid2 size="grow">
                            <Card variant="outlined" sx={{ minWidth: "30%", maxWidth: "100%" }}>
                                <CardHeader title={<Typography variant="body2">Service Account:</Typography>} />
                                <CardContent>
                                    <Typography>{plan.serviceAccount}</Typography>
                                </CardContent>
                            </Card>
                        </Grid2>
                    )}
                </Grid2>
            </Collapse >
            <CardActions>
                <Button
                    variant="contained"
                    color="primary"
                    startIcon={expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    onClick={() => { setExpanded(plan.planId, !expanded) }}
                    sx={{ marginTop: "16px" }}
                >
                    {expanded ? "Hide Details" : "Show Details"}
                </Button>
            </CardActions>
        </PlanCard >
    );
};

const InputPointCard: React.FC<{
    mountpoint: Mountpoint,
    variant?: "outlined" | "elevation",
    elevation?: number,
    children?: React.ReactNode,
}> = ({ mountpoint, variant = "outlined", elevation = 1, children }) => {
    return (
        <Card variant={variant} elevation={elevation} sx={{ margin: "16px" }}>
            <CardHeader
                avatar={<Tooltip title="input"><InputIcon /></Tooltip>}
                subheader="Input"
                title={`${mountpoint.path}`}
            />
            <CardContent>
                <Stack spacing={2}>
                    <TagSet tags={mountpoint.tags} />
                    {children}
                </Stack>
            </CardContent>
        </Card>
    );
}

const OutputPointCard: React.FC<{
    mountpoint: Mountpoint,
    variant?: "outlined" | "elevation",
    elevation?: number,
    children?: React.ReactNode
}> = ({ mountpoint, variant = "outlined", elevation = 1, children }) => {
    return (
        <Card variant={variant} elevation={elevation} sx={{ margin: "16px" }}>
            <CardHeader
                avatar={<Tooltip title="output"><OutputIcon /></Tooltip>}
                subheader="Output"
                title={`${mountpoint.path}`}
            />
            <CardContent>
                <Stack spacing={2}>
                    <TagSet tags={mountpoint.tags} />
                    {children}
                </Stack>
            </CardContent>
        </Card>
    );
}

const LogPointCard: React.FC<{
    log: LogPoint,
    variant?: "outlined" | "elevation",
    elevation?: number,
    children?: React.ReactNode
}> = ({ log, variant = "outlined", elevation = 1, children }) => {
    return (
        <Card variant={variant} elevation={elevation} sx={{ margin: "16px" }}>
            <CardHeader
                avatar={<Tooltip title="output"><OutputIcon /></Tooltip>}
                title="(log)"
            />
            <CardContent>
                <Stack spacing={2}>
                    <TagSet tags={log.tags} />
                    {children}
                </Stack>
            </CardContent>
        </Card>
    );
}

const RunCard = ({
    run,
    variant = "outlined",
    elevation = 1,
    action,
    children,
}: {
    run: RunSummary,
    variant?: "outlined" | "elevation",
    elevation?: number,
    action?: React.ReactNode,
    children?: React.ReactNode,
}) => {
    const allch = React.Children.toArray(children);

    const content: React.ReactNode[] = [];
    const actions: React.ReactNode[] = [];

    for (let c of allch) {
        if (React.isValidElement(c) && c.type === CardActions) {
            actions.push(c);
        } else {
            content.push(c);
        }
    }

    let icon = <ConstructionIcon />
    switch (run.status) {
        case "deactivated":
            icon = <PendingIcon />
            break;
        case "running":
            icon = <PlayIcon />
            break;
        case "aborting":
        case "failed":
        case "invalidated":
            icon = <ErrorIcon />
            break;
        case "completing":
        case "done":
            icon = <DoneIcon />
            break;
    }

    return (
        <Card variant={variant} elevation={elevation} sx={{ margin: "16px" }}>
            <CardHeader
                avatar={<Tooltip title="Run">{icon}</Tooltip>}
                title={`Run ID: ${run.runId}`}
                subheader={`Run / status: ${run.status}`}
                action={action}
            />
            <CardContent>
                <TableContainer>
                    <Table>
                        <TableBody>
                            <TableRow>
                                <TableCell><Typography variant="subtitle1">Updated At</Typography></TableCell>
                                <TableCell>{run.updatedAt.toLocaleString()}</TableCell>
                            </TableRow>
                            {
                                run.exit && (
                                    <>
                                        <TableRow>
                                            <TableCell><Typography variant="subtitle1">Exit Code</Typography></TableCell>
                                            <TableCell>{run.exit.code}</TableCell>
                                        </TableRow>
                                        {
                                            run.exit.message && (
                                                <TableRow>
                                                    <TableCell><Typography variant="subtitle1">Exit Message</Typography></TableCell>
                                                    <TableCell>{run.exit.message}</TableCell>
                                                </TableRow>
                                            )
                                        }
                                    </>
                                )
                            }
                        </TableBody>
                    </Table>
                </TableContainer>
                <PlanSummaryCard plan={run.plan} />
                {content}
                {actions}
            </CardContent>
        </Card >
    )
};

const RunItem: React.FC<{
    run: RunDetail,
    variant?: "outlined" | "elevation",
    elevation?: number,
    action?: React.ReactNode,
    expanded: boolean,
    setExpanded: (runId: string, mode: boolean) => void,
    logExpanded: boolean,
    setLogExpanded: (runId: string, mode: boolean) => void,
    runService: RunService,
}> = ({
    run,
    variant,
    elevation,
    action,
    expanded,
    setExpanded,
    runService,
    logExpanded,
    setLogExpanded,
}) => {
        return (
            <RunCard run={run} action={action} variant={variant} elevation={elevation}>
                <Collapse in={expanded} timeout="auto" unmountOnExit>
                    <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                        Inputs:
                    </Typography>
                    <Stack sx={{ marginLeft: "16px" }}>
                        {run.inputs.map((input) => (
                            <InputPointCard key={input.path} mountpoint={input}>
                                <Stack direction="row" alignItems="center" spacing={1}>
                                    <Chip icon={<StorageIcon />} label="Knit ID" />
                                    <Typography>{input.knitId}</Typography>
                                </Stack>
                            </InputPointCard>
                        ))}
                    </Stack>

                    <Typography variant="subtitle1" sx={{ marginTop: "16px" }}>
                        Outputs:
                    </Typography>
                    <Stack sx={{ marginLeft: "16px" }}>
                        {run.outputs.map((output) => (
                            <OutputPointCard key={output.path} mountpoint={output}>
                                <Stack direction="row" alignItems="center" spacing={1}>
                                    <Chip icon={<StorageIcon />} label="Knit ID" />
                                    <Typography>{output.knitId}</Typography>
                                </Stack>
                            </OutputPointCard>
                        ))}
                        {run.log && (
                            <LogPointCard log={run.log}>
                                <Stack direction="row" alignItems="center" spacing={1}>
                                    <Chip icon={<StorageIcon />} label="Knit ID" />
                                    <Typography>{run.log.knitId}</Typography>
                                </Stack>
                            </LogPointCard>
                        )}
                    </Stack>
                </Collapse>
                <Collapse in={logExpanded} timeout="auto" unmountOnExit>
                    <RunLogViewer runId={run.runId} runService={runService} />
                </Collapse>
                <CardActions>
                    <Button
                        variant="contained"
                        color="primary"
                        startIcon={expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                        onClick={() => { setExpanded(run.runId, !expanded) }}
                        sx={{ marginTop: "16px" }}
                    >
                        {expanded ? "Hide Details" : "Show Details"}
                    </Button>
                    {
                        (
                            run.log
                            && (
                                run.status === "running"
                                || run.status === "aborting"
                                || run.status === "completing"
                                || run.status === "done"
                                || run.status === "failed"
                            )
                        ) &&
                        <Button
                            variant="contained"
                            color="primary"
                            startIcon={logExpanded ? <ExpandLessIcon /> : <SubjectIcon />}
                            onClick={() => { setLogExpanded(run.runId, !logExpanded) }}
                            sx={{ marginTop: "16px" }}
                        >
                            {logExpanded ? "Hide Logs" : "Show Logs"}
                        </Button>
                    }
                </CardActions>
            </RunCard>
        );
    };

export type RunLogViewerProps = {
    runId: string;
    runService: RunService;
};

const RunLogViewer: React.FC<RunLogViewerProps> = ({ runId, runService }) => {
    const [logs, setLogs] = useState<string>("");
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const abortController = new AbortController();

        const fetchLogs = () => {
            runService.fetchLog(
                runId,
                (chunk) => {
                    setLoading(false);
                    setError(null);
                    setLogs(chunk);
                },
                abortController.signal,
            )
                .then(() => {
                    setLoading(false);
                    setError(null);
                })
                .catch((err) => {
                    if (err.name === "AbortError") {
                        setError("Request aborted");
                        setLoading(false);
                        return
                    }

                    // retry on 400 and 404. The Run might not be ready yet.
                    if (err.response?.status === 400 || err.response?.status === 404) {
                        const d = JSON.parse(err.response?.data ?? "{}");
                        setError(d.message ?? err.response?.statusText);
                        setTimeout(() => { fetchLogs(); }, 5000);
                        return
                    }

                    setError("Failed to fetch logs");
                    setLoading(false);
                });
        };
        fetchLogs();
        return () => {
            abortController.abort();
        };
    }, [runId, runService]);

    return (
        <Card variant="outlined">
            <CardHeader
                avatar={<SubjectIcon />}
                title="Logs"
            />
            <CardContent>
                {loading && <Typography variant="body2" fontStyle="italic">Loading logs...</Typography>}
                {error && <Typography color="error">{error}</Typography>}
                {
                    !loading && !error &&
                    <Paper sx={{ padding: 2, maxHeight: "100vh", overflow: "auto" }}>
                        <Box component="pre" sx={{ whiteSpace: "pre-wrap" }}>
                            {logs}
                        </Box>
                    </Paper>
                }
            </CardContent>
        </Card>
    );
};

export default RunLogViewer;

export {
    DataItem,
    DataCard,
    PlanItem,
    PlanCard,
    InputPointCard,
    OutputPointCard,
    LogPointCard,
    RunItem,
    RunCard,
};
