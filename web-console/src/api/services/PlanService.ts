import { ApiClient } from '../apiClient';
import { PlanDetail, Tag } from '../../types/types';
import { RawPlanDetail, toPlanDetail, toTagString } from './types/types';

export class PlanService {
    private apiClient: ApiClient;

    constructor(apiClient: ApiClient) {
        this.apiClient = apiClient;
    }

    public async fetchList(params: {
        active?: boolean;
        inTags?: Tag[];
        outTags?: Tag[];
        image?: string;
    } = {}): Promise<PlanDetail[]> {
        const queryParams = new URLSearchParams();

        if (params.active !== undefined) {
            queryParams.append('active', String(params.active));
        }
        if (params.inTags) {
            params.inTags.forEach(tag => queryParams.append('in_tag', toTagString(tag)));
        }
        if (params.outTags) {
            params.outTags.forEach(tag => queryParams.append('out_tag', toTagString(tag)));
        }
        if (params.image) {
            queryParams.append('image', params.image);
        }

        return this.apiClient
            .get<RawPlanDetail[]>(`/plans?${queryParams.toString()}`)
            .then(ps => ps.map(toPlanDetail));
    }

    public async fetchById(id: string): Promise<PlanDetail> {
        return this.apiClient
            .get<RawPlanDetail>(`/plans/${id}`)
            .then(toPlanDetail);
    }
}
