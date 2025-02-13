import { ApiClient } from '../apiClient';
import { DataDetail, Tag } from '../../types/types';
import { RawDataDetail, TagString, toDataDetail, toTagString } from './types/types';
import { Duration, durationToString } from './types/time';
import luxon from 'luxon';

export class DataService {
    private apiClient: ApiClient;

    constructor(apiClient: ApiClient) {
        this.apiClient = apiClient;
    }

    /**
     * Fetches a list of Data items from the API
     *
     * @param params - An object containing optional parameters for filtering the list of Data items.
     *
     * The `tags` field is an array of TagStrings.
     * If provided, only Data items with all of the specified tags will be returned.
     *
     * The `since` field is a DateTime.
     * If provided, only Data items created after the specified time will be returned.
     * If the `since` field is invalid, it will be ignored.
     *
     * The `duration` field is a Duration.
     * If provided, only Data items created within the specified time range from since will be returned.
     * If `sicne` is not provided or invalid, the `duration` field will be ignored.
     *
     * @returns a Promise that resolves to an array of DataDetail objects.
     */
    public async fetchList(params: {
        tags?: Tag[];
        since?: luxon.DateTime;
        duration?: Duration;
    } = {}): Promise<DataDetail[]> {
        const queryParams = new URLSearchParams();

        if (params.tags) {
            params.tags.forEach(tag => queryParams.append('tag', toTagString(tag)));
        }
        if (params.since && params.since.isValid) {
            const since = params.since.toISO();
            if (since) {
                queryParams.append('since', since);
            }
            if (params.duration) {
                queryParams.append('duration', durationToString(params.duration));
            }
        }

        return this.apiClient
            .get<RawDataDetail[]>(`/data/?${queryParams.toString()}`)
            .then(ps => ps.map(toDataDetail));
    }

    /**
     * Fetches a single Data item from the API by its ID
     *
     * @param id - The ID of the Data item to fetch
     *
     * @returns a Promise that resolves to a DataDetail object with the specified ID.
     * If no Data item with the specified ID exists, the Promise will be rejected.
     */
    public async fetchById(id: string): Promise<DataDetail> {
        const queryParams = new URLSearchParams();
        queryParams.append('tag', `knit#id:${id}`);

        return this.apiClient
            .get<RawDataDetail[]>(`/data?${queryParams.toString()}`)
            .then((ds) => {
                if (ds.length === 0) {
                    throw new Error(`No data found with ID ${id}`);
                }
                return toDataDetail(ds[0]);
            });
    }
}
