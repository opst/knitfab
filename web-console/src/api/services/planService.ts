import { PlanDetail, Tag } from '../../types/types';
import { ApiClient } from '../apiClient';
import { RawPlanDetail, toPlanDetail, toTagString } from './types/types';

export class PlanService {
    private apiClient: ApiClient;

    /**
     *
     * @param apiClient
     */
    constructor(apiClient: ApiClient) {
        this.apiClient = apiClient;
    }

    /**
     * Fetches a list of PlanDetails from the API
     *
     * @param params filter parameters
     * @returns found PlanDetails
     */
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
            .get<RawPlanDetail[]>(`/plans/?${queryParams.toString()}`)
            .then(ps => ps.map(toPlanDetail));
    }

    /**
     * Fetches a PlanDetail by its ID
     *
     * @param id Plan ID to fetch
     * @returns PlanDetail found
     */
    public async fetchById(id: string): Promise<PlanDetail> {
        return this.apiClient
            .get<RawPlanDetail>(`/plans/${id}`)
            .then(toPlanDetail);
    }
}
