export type Duration = {
    hours?: number
    minutes?: number
    seconds?: number
}

export const durationToString = (duration: Duration): string => {
    const parts = [];
    if (duration.hours !== undefined) {
        parts.push(`${duration.hours}h`);
    }
    if (duration.minutes !== undefined) {
        parts.push(`${duration.minutes}m`);
    }
    if (duration.seconds !== undefined) {
        parts.push(`${duration.seconds}s`);
    }
    return parts.join('');
}
