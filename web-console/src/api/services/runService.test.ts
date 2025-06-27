import { RunService } from "./runService";
import { ApiClient } from "../apiClient";
import { Duration } from "./types/time";
import { DateTime } from "luxon";
import { RunStatus } from "./types/types";

// モック用の ApiClient を作成
const mockApiClient: Partial<ApiClient> = {
    get: jest.fn(),
    put: jest.fn(),
    getStream: jest.fn(),
};

describe("RunService", () => {
    let runService: RunService;

    beforeEach(() => {
        jest.clearAllMocks();
        runService = new RunService(mockApiClient as ApiClient);
    });

    describe("fetchList", () => {
        it("should send request with correct URL and query parameters", async () => {
            const planId = ["plan1", "plan2"];
            const knitIdInput = ["dataInA", "dataInB"];
            const knitIdOutput = ["dataOutA", "dataOutB"];
            const status: RunStatus[] = ["running", "done"];
            const since = DateTime.fromISO("2024-02-10T10:00:00Z");
            const duration: Duration = { hours: 1, minutes: 2, seconds: 3 };

            (mockApiClient.get as jest.Mock).mockResolvedValue([]);
            await runService.fetchList({
                planId, knitIdInput, knitIdOutput, status, since, duration,
            });

            expect(mockApiClient.get).toHaveBeenCalledTimes(1);
            const calledUrl = URL.parse(
                (mockApiClient.get as jest.Mock).mock.calls[0][0] as string,
                "http://localhost",
            );
            expect(calledUrl?.pathname).toBe("/runs/");
            expect(calledUrl?.searchParams.getAll("plan")).toContain("plan1,plan2");
            expect(calledUrl?.searchParams.getAll("knitIdInput")).toContain("dataInA,dataInB");
            expect(calledUrl?.searchParams.getAll("knitIdOutput")).toContain("dataOutA,dataOutB");
            expect(calledUrl?.searchParams.getAll("status")).toContain("running,done");
            expect(calledUrl?.searchParams.getAll("since")).toContain(since.toISO());
            expect(calledUrl?.searchParams.getAll("duration")).toContain("1h2m3s");
        });

        it("should handle missing parameters with defaults", async () => {
            (mockApiClient.get as jest.Mock).mockResolvedValue([]);
            await runService.fetchList();
            const calledUrl = (mockApiClient.get as jest.Mock).mock.calls[0][0] as string;
            expect(calledUrl).toBe("/runs/?");
        });
    });

    describe("fetchById", () => {
        it("should call correct URL for fetchById", async () => {
            const id = "run123";
            (mockApiClient.get as jest.Mock).mockResolvedValue({
                runId: id,
                status: "running",
                updatedAt: "2024-02-10T10:00:00Z",
                plan: { planId: "plan123" },
                inputs: [],
                outputs: [],
            });
            await runService.fetchById(id);
            expect(mockApiClient.get).toHaveBeenCalledWith(`/runs/${id}`);
        });
    });

    describe("fetchLog", () => {
        it("should call getStream with correct URL and options", async () => {
            const runId = "run123";
            const onData = jest.fn();
            const fakeSignal = {} as AbortSignal;
            (mockApiClient.getStream as jest.Mock).mockResolvedValue(undefined);
            await runService.fetchLog(runId, onData, fakeSignal);
            expect(mockApiClient.getStream).toHaveBeenCalledTimes(1);

            const mockGetStream = mockApiClient.getStream as jest.Mock;
            const calledUrl = mockGetStream.mock.calls[0][0] as string;
            const calledOnData = mockGetStream.mock.calls[0][1];
            const options = mockGetStream.mock.calls[0][2];
            expect(calledUrl).toBe(`/runs/${runId}/log?follow`);
            expect(calledOnData).toBe(onData);
            expect(options).toEqual({ signal: fakeSignal });
        });
    });

    describe("stopRun", () => {
        it("should call stop endpoint with correct parameters", async () => {
            const runId = "run123";
            const asFailed = true;
            (mockApiClient.put as jest.Mock).mockResolvedValue({
                runId,
                status: "stopped",
                updatedAt: "2024-02-10T10:00:00Z",
                plan: { planId: "plan123" },
                inputs: [],
                outputs: [],
            });

            const result = await runService.stopRun(runId, asFailed);
            expect(mockApiClient.put).toHaveBeenCalledWith(`/runs/${runId}/abort`, null);
            expect(result.runId).toBe(runId);
        });

        it("should default asFailed to false if not provided", async () => {
            const runId = "run123";
            const asFailed = false;
            (mockApiClient.put as jest.Mock).mockResolvedValue({
                runId,
                status: "stopped",
                updatedAt: "2024-02-10T10:00:00Z",
                plan: { planId: "plan123" },
                inputs: [],
                outputs: [],
            });

            const result = await runService.stopRun(runId, asFailed);
            expect(mockApiClient.put).toHaveBeenCalledWith(`/runs/${runId}/tearoff`, null);
            expect(result.runId).toBe(runId);
        });
    });

    describe("retryRun", () => {
        it("should call retry endpoint and return null", async () => {
            const runId = "run123";
            (mockApiClient.put as jest.Mock).mockResolvedValue(null);
            const result = await runService.retryRun(runId);
            expect(mockApiClient.put).toHaveBeenCalledWith(`/runs/${runId}/retry`, null);
            expect(result).toBeNull();
        });
    });
});
