import { useState } from "react";
import { Tag } from "../types/types"
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import TextField from "@mui/material/TextField";
import { TagString } from "../api/services/types/types";

function isTagString(tag: string): tag is TagString {
    return tag.includes(':');
}

function isSystemTag(tag: TagString): boolean {
    return tag.startsWith("knit#");
}

function parseTag(tag: TagString): Tag {
    const [key, value] = tag.split(/(?<=^[^:]*):/, 2);
    return { key: key.trim(), value: value.trim() };
}

export const TagInput = ({ label, onSave, variant = "contained", allowSystemTags=false }: {
    label: string
    onSave?: (tag: Tag) => void,
    variant?: "contained" | "outlined",
    allowSystemTags?: boolean
}) => {
    const [text, setText] = useState("");

    return (
        <Stack direction="row" spacing={1} alignItems="center">
            <TextField
                label={label}
                value={text}
                onChange={(e) => setText(e.target.value)}
                variant="filled"
                fullWidth
            />
            <Button
                variant={variant}
                onClick={() => {
                    if (!isTagString(text) || (!allowSystemTags && isSystemTag(text))) {
                        return;
                    }
                    const newTag = parseTag(text);
                    onSave && onSave(newTag);
                    setText("");
                }}
                disabled={!isTagString(text) || (!allowSystemTags && isSystemTag(text))}
                >
                Add
            </Button>
        </Stack>
    )
}