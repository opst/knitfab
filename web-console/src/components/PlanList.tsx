import AutoModeIcon from "@mui/icons-material/AutoMode";
import CheckboxIcon from "@mui/icons-material/CheckBox";
import CheckboxBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank";
import ClearIcon from "@mui/icons-material/Clear";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Inventory2Icon from "@mui/icons-material/Inventory2";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import RefreshIcon from "@mui/icons-material/Refresh";
import TagIcon from "@mui/icons-material/Tag";
import Alert from "@mui/material/Alert";
import Badge from "@mui/material/Badge";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import ButtonGroup from "@mui/material/ButtonGroup";
import Chip from "@mui/material/Chip";
import Collapse from "@mui/material/Collapse";
import Divider from "@mui/material/Divider";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import React, { useCallback, useEffect, useState } from "react";
import { PlanService } from "../api/services/planService";
import { toTagString } from "../api/services/types/types";
import { PlanDetail, Tag, tagsEqual } from "../types/types";
import { TagInput } from "./Inputs";
import { PlanItem, TagChip } from "./Items";

export type PlanListProps = {
    planService: PlanService;
    setPlanGraphRoot: (planId: string) => void;
};

const PlanList: React.FC<PlanListProps> = ({ planService, setPlanGraphRoot }) => {
    const [planList, setPlanList] = useState<PlanDetail[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);
    const [autoRefresh, setAutoRefresh] = useState<boolean>(false);
    const [expanded, setExpanded] = useState<Set<string>>(new Set());
    const [filter, setFilter] = useState<PlanFilterParams>({
        inTags: [],
        outTags: [],
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

    const fetchData = async () => {
        setLoading(true);
        try {
            const response = await planService.fetchList(filter);
            setPlanList(response);
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
    }, [planService, filter]);

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
                        filter.active === undefined &&
                        filter.inTags.length === 0 &&
                        filter.outTags.length === 0 &&
                        filter.image === ""
                    }
                    color="primary">
                    <Button
                        onClick={() => setFilterIsVisible(!filterIsVisible)}
                        startIcon={filterIsVisible ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    >
                        {filterIsVisible ? "Hide Filters" : "Show Filters"}
                    </Button>
                </Badge>
                <Box flexGrow={1} >
                    <Collapse in={filterIsVisible}>
                        <PlanFilter value={filter} onChange={setFilter} />
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
                        {planList.map((plan) => (
                            <PlanItem
                                key={plan.planId}
                                action={
                                    <Button
                                        variant="contained"
                                        endIcon={<OpenInNewIcon />}
                                        onClick={() => setPlanGraphRoot(plan.planId)}
                                    >
                                        Plan Graph
                                    </Button>
                                }
                                plan={plan}
                                expanded={expanded.has(plan.planId)}
                                setExpanded={updateExpanded}
                            />
                        ))}
                    </Stack>
                )
            }
        </Stack>
    );
};

type PlanFilterParams = {
    active?: boolean;
    inTags: Tag[];
    outTags: Tag[];
    image?: string;
}

type PlanFilterProps = {
    value: PlanFilterParams;
    onChange: (filters: PlanFilterParams) => void;
};

const PlanFilter: React.FC<PlanFilterProps> = ({ value, onChange }) => {
    const [imageInput, setImageInput] = useState<string>(value.image || "");
    const [imageTimeout, setImageTimeout] = useState<NodeJS.Timeout | null>(null);

    const handleImageChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setImageInput(e.target.value);
        if (imageTimeout) {
            clearTimeout(imageTimeout);
            setImageTimeout(null);
        }
        setImageTimeout(
            setTimeout(() => {
                onChange({ ...value, image: e.target.value });
                setImageTimeout(null);
            }, 1000)
        );
    };

    const setActive = (active: boolean | undefined) => {
        onChange({ ...value, active });
    };

    const removeTag = (key: "inTags" | "outTags", tag: Tag) => {
        onChange({ ...value, [key]: value[key].filter(t => !tagsEqual(t, tag)) });
    };

    const [key, setKey] = useState(1);

    return (
        <Stack key={key} spacing={2}>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip icon={<CheckboxIcon />} label="Active Status" />
                <Select
                    value={value.active === undefined ? "undefined" : value.active ? "true" : "false"}
                    onChange={(e) => setActive(e.target.value === "undefined" ? undefined : e.target.value === "true")}
                    label="Active Status"
                    variant="filled"
                >
                    <MenuItem value="undefined">Any</MenuItem>
                    <MenuItem value="true">Active</MenuItem>
                    <MenuItem value="false">Inactive</MenuItem>
                </Select>
            </Stack>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip icon={<Inventory2Icon />} label="Image" />
                <TextField
                    label="Image"
                    variant="filled"
                    value={imageInput || ""}
                    onChange={handleImageChange}
                    fullWidth
                />
            </Stack>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip icon={<TagIcon />} label="Tags for Inputs" />
                {value.inTags.map(tag => <TagChip key={toTagString(tag)} tag={tag} onDelete={() => removeTag("inTags", tag)} />)}
                <TagInput
                    label="Add Input Tag"
                    onSave={(tag) => {
                        if (!value.inTags.find(t => tagsEqual(t, tag))) {
                            onChange({ ...value, inTags: [...value.inTags, tag] });
                        }
                    }}
                    allowSystemTags
                />
            </Stack>
            <Stack direction="row" spacing={1} alignItems="center">
                <Chip icon={<TagIcon />} label="Tags for Outputs" />
                {value.outTags.map(tag => <TagChip key={toTagString(tag)} tag={tag} onDelete={() => removeTag("outTags", tag)} />)}
                <TagInput
                    label="Add Output Tag"
                    onSave={(tag) => {
                        if (!value.outTags.find(t => tagsEqual(t, tag))) {
                            onChange({ ...value, outTags: [...value.outTags, tag] });
                        }
                    }}
                    allowSystemTags
                />
            </Stack>
            <Box>
                <Button
                    onClick={() => {
                        onChange({ inTags: [], outTags: [] });
                        setImageInput("");
                        setKey((k) => -k); // force re-render
                    }}
                    variant="contained"
                    startIcon={<ClearIcon />}
                >
                    Clear
                </Button>
            </Box>
        </Stack>
    );
};

export default PlanList;
