import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';

export class ApiClient {
    private client: AxiosInstance;

    constructor(baseURL: string) {
        this.client = axios.create({
            adapter: "xhr",
            baseURL,
            headers: {
                'Content-Type': 'application/json',
            },
        });
    }

    /**
     *  send GET request
     */
    public async get<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
        const response = await this.client.get<T>(url, config);
        return response.data;
    }

    /**
     * send POST request
     */
    public async post<T, U>(url: string, data: U, config?: AxiosRequestConfig): Promise<T> {
        const response = await this.client.post<T>(url, data, config);
        return response.data;
    }

    /**
     *
     * receive a text stream
     *
     * @param url
     * @param onData callback when a new chunk of data is received
     * @param config
     * @returns a Promise that resolves when the stream is closed
     */
    public async getStream(url: string, onData: (chunk: string) => void, config?: AxiosRequestConfig): Promise<void> {
        const response = this.client.get(
            url,
            {
                ...config,
                responseType: 'stream',
                onDownloadProgress: (progressEvent) => {
                    let eventObj: XMLHttpRequest | undefined = undefined;
                    if (progressEvent.event?.currentTarget) {
                        eventObj = progressEvent.event?.currentTarget;
                    } else if (progressEvent.event?.srcElement) {
                        eventObj = progressEvent.event?.srcElement;
                    } else if (progressEvent.event?.target) {
                        eventObj = progressEvent.event?.target;
                    }
                    if (!eventObj) return;
                    onData(eventObj.response);
                },
            },
        );

        return new Promise((resolve, reject) => {
            response.then(() => { resolve() }).catch(reject);
        });
    }
}
