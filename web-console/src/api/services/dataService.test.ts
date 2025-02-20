import { DataService } from "./dataService";
import { ApiClient } from "../apiClient";
import { Duration } from "./types/time";
import { DateTime } from "luxon";
import { RawDataDetail } from "./types/types";

const mockApiClient: Partial<ApiClient> = {
    get: jest.fn(),
};

describe("DataService", () => {
    let testee: DataService;

    beforeEach(() => {
        jest.clearAllMocks();
        testee = new DataService(mockApiClient as ApiClient);
    });

    describe("fetchList", () => {
        it("should send request with correct URL and query parameters", async () => {
            const tags = [{ key: "a", value: "foo" }, { key: "b", value: "bar" }];
            const since = DateTime.fromISO("2024-02-10T10:00:00Z");
            const duration: Duration = { hours: 1, minutes: 2, seconds: 3 };

            (mockApiClient.get as jest.Mock).mockResolvedValue([]);

            await testee.fetchList({ tags, since, duration });

            expect(mockApiClient.get).toHaveBeenCalledTimes(1);
            const calledUrl = URL.parse(
                (mockApiClient.get as jest.Mock).mock.calls[0][0] as string,
                "http://localhost"
            );
            expect(calledUrl).not.toBeNull();
            expect(calledUrl?.pathname).toBe("/data/");

            const gotTags = calledUrl?.searchParams.getAll("tag");
            expect(gotTags).toContain("a:foo");
            expect(gotTags).toContain("b:bar");

            expect(calledUrl?.searchParams.getAll("since")).toContain(since.toISO());
            expect(calledUrl?.searchParams.getAll("duration")).toContain("1h2m3s");
        });

        it("should handle missing parameters with defaults", async () => {
            (mockApiClient.get as jest.Mock).mockResolvedValue([]);

            await testee.fetchList();
            const calledUrl = (mockApiClient.get as jest.Mock).mock.calls[0][0] as string;
            expect(calledUrl).toBe("/data/?");
        });
    });

    describe("fetchById", () => {
        it("should call correct URL for fetchById", async () => {
            const id = "data-123";
            (mockApiClient.get as jest.Mock).mockResolvedValue([
                {
                    knitId: "data-123",
                    tags: [],
                    upstream: {
                        run: {
                            runId: "run-123",
                            status: "completed",
                            updatedAt: "2025-02-10T10:00:00Z",
                            plan: {
                                planId: "plan-123",
                                entrypoint: [],
                                args: [],
                                annotations: [],
                            },
                        },
                    },
                    downstreams: [],
                    nomination: [],
                } satisfies RawDataDetail,
            ]);
            await testee.fetchById(id);

            const calledUrl = URL.parse(
                (mockApiClient.get as jest.Mock).mock.calls[0][0] as string,
                "http://localhost"
            );
            expect(calledUrl).not.toBeNull();
            expect(calledUrl?.pathname).toBe(`/data/`);
            expect(calledUrl?.searchParams.get("tag")).toContain("knit#id:data-123");
        });
    });
});
