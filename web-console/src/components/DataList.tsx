import AutoModeIcon from "@mui/icons-material/AutoMode";
import CheckboxIcon from "@mui/icons-material/CheckBox";
import CheckboxBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import ClearIcon from "@mui/icons-material/Clear";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import RefreshIcon from "@mui/icons-material/Refresh";
import TagIcon from "@mui/icons-material/Tag";
import TodayIcon from "@mui/icons-material/Today";
import Alert from "@mui/material/Alert";
import Badge from "@mui/material/Badge";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import ButtonGroup from "@mui/material/ButtonGroup";
import Chip from "@mui/material/Chip";
import Collapse from "@mui/material/Collapse";
import Divider from "@mui/material/Divider";
import Grid from "@mui/material/Grid";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { DateTimeField } from "@mui/x-date-pickers/DateTimeField";
import React, { useCallback, useEffect, useState } from "react";
import { DataService } from "../api/services/dataService";
import { Duration } from "../api/services/types/time";
import { isTagString, parseTag, toTagString } from "../api/services/types/types";
import { DataDetail, Tag, tagsEqual } from "../types/types";
import { DurationFilter } from "./Filter";
import { TagInput } from "./Inputs";
import { DataItem, TagChip } from "./Items";

export type DataListProps = {
    /**
     * Data service to fetch data from.
     */
    dataService: DataService;

    /**
     * Callback to set the Knit ID (Data) as the lineage graph root.
     * @param knitId
     */
    setLineageGraphRoot: (knitId: string) => void;
};

/**
 * List View for Data.
 * @param param0
 * @returns
 */
const DataList: React.FC<DataListProps> = ({ dataService, setLineageGraphRoot }) => {
    /**
     * DataDetails which are fetched from the server.
     */
    const [dataList, setDataList] = useState<DataDetail[]>([]);

    /**
     * Loading state.
     */
    const [loading, setLoading] = useState<boolean>(true);

    /**
     * Error message.
     */
    const [error, setError] = useState<string | null>(null);

    /**
     * Auto refresh is active or not.
     */
    const [autoRefresh, setAutoRefresh] = useState<boolean>(false);

    /**
     * Knit IDs which are expanded.
     */
    const [expanded, setExpanded] = useState<Set<string>>(new Set());

    /**
     * What are the filters applied.
     */
    const [filter, setFilter] = useState<DataFilterParams>({
        tags: [],
        duration: {},
    });

    /**
     * Filter visibility.
     *
     * If trur, filter will be visible.
     */
    const [filterIsVisible, setFilterIsVisible] = useState<boolean>(false);

    /**
     * Update the expanded state of a knit ID.
     *
     * @param knitId KnitId points the target Data
     * @param mode Expanded(true) or not(false).
     */
    const updateExpanded = useCallback((knitId: string, mode: boolean) => {
        if (mode) {
            setExpanded((prev) => {
                const next = new Set(prev);
                next.add(knitId);
                return next;
            });
        } else {
            setExpanded((prev) => {
                const next = new Set(prev);
                next.delete(knitId);
                return next;
            });
        }
    }, [setExpanded])

    /**
     * Fetch Data from the server with the applied filters.
     */
    const fetchData = async () => {
        setLoading(true);
        try {
            const response = await dataService.fetchList(filter);
            setDataList(response);
            setError(null);
        } catch (err) {
            console.error("Error fetching data:", err);
            setError("Error fetching data");
        } finally {
            setLoading(false);
        }
    };

    // Update list when filter changes.
    useEffect(() => {
        fetchData();
    }, [dataService, filter]);

    // Auto refresh mode: Fetch data every 30 seconds.
    useEffect(() => {
        if (autoRefresh) {
            fetchData();
            const interval = setInterval(fetchData, 30000);
            return () => clearInterval(interval);
        }
    }, [autoRefresh, filter]);

    return (
        <Stack spacing={2}>
            <Stack spacing={2} direction="row">
                <Badge
                    invisible={
                        filter.tags.length === 0 &&
                        !filter.since &&
                        !filter.duration.hours !== undefined &&
                        !filter.duration.minutes !== undefined &&
                        !filter.duration.seconds !== undefined
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
                        <DataFilter value={filter} onChange={setFilter} />
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
                            Auto(30s)
                        </Button>
                    </ButtonGroup>
                </Box>
            </Stack>
            {error && <Alert severity="error">{error}</Alert>}
            {
                !error && (
                    <Stack key="item-list">
                        {dataList.map((data) => (
                            <DataItem
                                key={data.knitId}
                                action={
                                    <Button
                                        variant="contained"
                                        endIcon={<OpenInNewIcon />}
                                        onClick={() => { setLineageGraphRoot(data.knitId) }}
                                    >
                                        Lineage
                                    </Button>
                                }
                                data={data}
                                expanded={expanded.has(data.knitId)}
                                setExpanded={updateExpanded}
                                onTagsUpdate={change => {
                                    dataService.
                                        updateTags(data.knitId, change).
                                        then((data) => {
                                            setDataList((prev) => {
                                                return prev.map((d) => {
                                                    if (d.knitId === data.knitId) {
                                                        return data;
                                                    }
                                                    return d;
                                                });
                                            });
                                        });
                                }}
                            />
                        ))}
                    </Stack>
                )
            }
        </Stack>
    );
};

type DataFilterParams = {
    tags: Tag[];
    since?: luxon.DateTime;
    duration: Duration;
};

const DataFilter: React.FC<{
    value: DataFilterParams,
    onChange: (value: DataFilterParams) => void
}> = ({ value, onChange }) => {

    const removeTag = (tag: Tag) => {
        onChange({
            ...value,
            tags: value.tags.filter(t => !tagsEqual(t, tag)),
        });
    }
    const setSince = (since?: luxon.DateTime | null) => {
        onChange({ ...value, since: since ?? undefined });
    }

    const setDuration = (duration: Duration) => {
        if (!duration) {
            duration = {};
        }
        onChange({ ...value, duration: duration });
    }

    const [key, setKey] = useState(1);

    return (
        <Stack key={key} spacing={2}>
            <Grid container direction="row" spacing={1} alignItems={"center"}>
                <Grid>
                    <Chip label="Tags" icon={<TagIcon />} />
                </Grid>
                {value.tags.map((tag) => (
                    <Grid>
                        <TagChip
                            key={toTagString(tag)}
                            tag={tag}
                            onDelete={() => removeTag(tag)}
                        />
                    </Grid>
                ))}
                <Grid flexGrow={1} minWidth="50%">
                    <TagInput
                        label="Add Tag"
                        onSave={(tag) => {
                            if (value.tags.find(t => tagsEqual(t, tag))) {
                                return;
                            }
                            onChange({ ...value, tags: [...value.tags, tag] });
                        }}
                        allowSystemTags
                    />
                </Grid>
            </Grid>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip label="Timestamp" icon={<TodayIcon />} />
                <Typography alignContent="center">from</Typography>
                <DateTimeField
                    label="Since"
                    value={value.since ?? null}
                    format="yyyy-MM-dd HH:mm:ss"
                    ampm={false}
                    onChange={setSince}
                    clearable
                    variant="filled"
                    fullWidth
                />
            </Stack>
            <Stack direction="row" spacing={2} alignContent="flex-end">
                <Typography alignContent="center">to</Typography>
                <DurationFilter
                    value={value.duration}
                    onChange={setDuration}
                    disabled={!(value.since?.isValid)}
                    variant="filled"
                />
                <Typography alignContent="center">later</Typography>
            </Stack>
            <Box>
                <Button
                    onClick={() => {
                        onChange({ tags: [], duration: {} })
                        setKey((k) => -k);  // force re-render
                    }}
                    variant="contained"
                    startIcon={<ClearIcon />}
                >
                    Clear
                </Button>
            </Box>
        </Stack>
    )
}

export default DataList;
