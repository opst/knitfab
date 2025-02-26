import { InputAdornment, Stack, TextField, TextFieldVariants } from "@mui/material";
import React, { useEffect, useState } from "react";
import { Duration } from "../api/services/types/time";

const DurationFilterImpl: React.FC<{
    value: Duration | null,
    disabled?: boolean,
    variant?: TextFieldVariants,
    onChange: (value: Duration) => void,
}> = ({ value, disabled, variant, onChange }) => {
    const [inputInterval, setInputInterval] = useState<NodeJS.Timeout | null>(null);

    const [hours, setHours] = useState(value?.hours ?? null);
    const [minutes, setMinutes] = useState(value?.minutes ?? null);
    const [seconds, setSeconds] = useState(value?.seconds ?? null);

    const onHoursChange = (ev: React.ChangeEvent<HTMLInputElement>) => {
        if (inputInterval) {
            clearInterval(inputInterval);
        }

        let h: number | undefined = parseInt(ev.target.value);
        if (isNaN(h)) {
            setHours(null);
            h = undefined;
        } else {
            setHours(h);
        }

        setInputInterval(setTimeout(() => {
            const newDuration = {
                hours: h ?? undefined,
                minutes: minutes ?? undefined,
                seconds: seconds ?? undefined,
            };
            setInputInterval(null)
            onChange(newDuration);
        }, 500));
    }

    const setMinutesChange = (ev: React.ChangeEvent<HTMLInputElement>) => {
        if (inputInterval) {
            clearInterval(inputInterval);
        }

        let m: number | undefined = parseInt(ev.target.value);
        if (isNaN(m)) {
            setMinutes(null);
            m = undefined
        } else {
            setMinutes(m);
        }

        setInputInterval(setTimeout(() => {
            const newDuration = {
                hours: hours ?? undefined,
                minutes: m ?? undefined,
                seconds: seconds ?? undefined,
            };
            setInputInterval(null)
            onChange(newDuration);
        }, 500));
    }

    const setSecondsChange = (ev: React.ChangeEvent<HTMLInputElement>) => {
        if (inputInterval) {
            clearInterval(inputInterval);
        }

        let s: number | undefined = parseInt(ev.target.value);
        if (isNaN(s)) {
            setSeconds(null);
            s = undefined;
        } else {
            setSeconds(s);
        }

        setInputInterval(setTimeout(() => {
            const newDuration = {
                hours: hours ?? undefined,
                minutes: minutes ?? undefined,
                seconds: s ?? undefined,
            };
            setInputInterval(null)
            onChange(newDuration);
        }, 500));
    }

    useEffect(() => {
        return () => {
            if (inputInterval !== null) {
                clearInterval(inputInterval);
            }
        }
    })

    return (
        <Stack direction="row" spacing={2} alignContent="flex-end">
            <TextField
                disabled={disabled}
                label="Hours"
                type="number"
                variant={variant}
                value={hours ?? ""}
                onChange={onHoursChange}
                slotProps={{
                    input: {
                        endAdornment: <InputAdornment position="end">h</InputAdornment>
                    }
                }}
            />
            <TextField
                disabled={disabled}
                label="Minutes"
                type="number"
                variant={variant}
                value={minutes ?? ""}
                onChange={setMinutesChange}
                slotProps={{
                    input: {
                        endAdornment: <InputAdornment position="end">m</InputAdornment>
                    }
                }}
            />
            <TextField
                disabled={disabled}
                label="Seconds"
                type="number"
                variant={variant}
                value={seconds ?? ""}
                onChange={setSecondsChange}
                slotProps={{
                    input: {
                        endAdornment: <InputAdornment position="end">s</InputAdornment>
                    }
                }}
            />
        </Stack>
    );
}

export const DurationFilter: React.FC<{
    value: Duration | null,
    disabled?: boolean,
    onChange: (value: Duration) => void,
    variant?: TextFieldVariants,
}> = ({ value, disabled, onChange, variant }) => {

    const [lastValue, setLastValue] = useState<Duration | null>(value);
    const [key, setKey] = useState(0);

    useEffect(() => {
        if (value?.hours !== lastValue?.hours || value?.minutes !== lastValue?.minutes || value?.seconds !== lastValue?.seconds) {
            setLastValue(value);
            setKey((key) => key + 1);  // force re-render with new value
        }
    }, [value, lastValue, setKey]);

    return (
        <DurationFilterImpl
            key={key}
            value={value}
            disabled={disabled}
            variant={variant}
            onChange={onChange}
        />
    );
}
