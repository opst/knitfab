import { ApiClient } from '../apiClient';
import { RunDetail } from '../../types/types';
import { RawRunDetail, RunStatus, toRunDetail } from './types/types';
import { Duration, durationToString } from './types/time';
import { DateTime } from 'luxon';


export class RunService {
    private apiClient: ApiClient;

    constructor(apiClient: ApiClient) {
        this.apiClient = apiClient;
    }

    public async fetchList(params: {
        planId?: string[];
        knitIdInput?: string[];
        knitIdOutput?: string[];
        status?: RunStatus[];
        since?: DateTime;
        duration?: Duration;
    } = {}): Promise<RunDetail[]> {
        const queryParams = new URLSearchParams();

        if (params.planId && 0 < params.planId.length) {
            queryParams.append('plan', params.planId.join(','));
        }
        if (params.knitIdInput && 0 < params.knitIdInput.length) {
            queryParams.append('knitIdInput', params.knitIdInput.join(','));
        }
        if (params.knitIdOutput && 0 < params.knitIdOutput.length) {
            queryParams.append('knitIdOutput', params.knitIdOutput.join(','));
        }
        if (params.status && 0 < params.status.length) {
            queryParams.append('status', params.status.join(','));
        }
        {
            const since = params.since?.toISO();
            if (since) {
                queryParams.append('since', since);
            }
            if (params.duration) {
                queryParams.append('duration', durationToString(params.duration));
            }
        }

        return this.apiClient
            .get<RawRunDetail[]>(`/runs?${queryParams.toString()}`)
            .then(ps => ps.map(toRunDetail));
    }

    public async fetchById(id: string): Promise<RunDetail> {
        return this.apiClient
            .get<RawRunDetail>(`/runs/${id}`)
            .then(toRunDetail);
    }

    public async fetchLog(runId: string, onData: (chunk: string) => void, signal?: AbortSignal): Promise<void> {
        const url = `/runs/${runId}/log?follow`;
        return this.apiClient.getStream(url, onData, { signal });
    }
}
