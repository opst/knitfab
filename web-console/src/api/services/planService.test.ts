import { PlanService } from "./planService";
import { ApiClient } from "../apiClient";
import { Duration } from "./types/time";
import { DateTime } from "luxon";
import { RawPlanDetail } from "./types/types";

const mockApiClient: Partial<ApiClient> = {
    get: jest.fn(),
};

describe("PlanService", () => {
    let planService: PlanService;

    beforeEach(() => {
        jest.clearAllMocks();
        planService = new PlanService(mockApiClient as ApiClient);
    });

    describe("fetchList", () => {
        it("should send request with correct URL and query parameters", async () => {
            const inTags = [{ key: "a", value: "foo" }, { key: "b", value: "bar" }];
            const outTags = [{ key: "c", value: "foo" }, { key: "d", value: "bar" }];
            const active = true;
            const image = "image:tag";

            (mockApiClient.get as jest.Mock).mockResolvedValue([]);

            await planService.fetchList({ image: image, inTags: inTags, outTags: outTags, active });

            expect(mockApiClient.get).toHaveBeenCalledTimes(1);
            const calledUrl = URL.parse(
                (mockApiClient.get as jest.Mock).mock.calls[0][0] as string,
                "http://localhost",
            );
            expect(calledUrl).not.toBeNull();
            expect(calledUrl?.pathname).toBe("/plans/");
            expect(calledUrl?.searchParams.getAll("image")).toContain(image);
            expect(calledUrl?.searchParams.getAll("in_tag")).toContain("a:foo");
            expect(calledUrl?.searchParams.getAll("in_tag")).toContain("b:bar");
            expect(calledUrl?.searchParams.getAll("out_tag")).toContain("c:foo");
            expect(calledUrl?.searchParams.getAll("out_tag")).toContain("d:bar");
            expect(calledUrl?.searchParams.getAll("active")).toContain("true");
        });

        it("should handle missing parameters with defaults", async () => {
            (mockApiClient.get as jest.Mock).mockResolvedValue([]);

            await planService.fetchList();
            const calledUrl = (mockApiClient.get as jest.Mock).mock.calls[0][0] as string;
            expect(calledUrl).toBe("/plans/?");
        });
    });

    describe("fetchById", () => {
        it("should call correct URL for fetchById", async () => {
            const id = "plan-123";
            (mockApiClient.get as jest.Mock).mockResolvedValue({
                planId: id,
                image: "image:tag",
                entrypoint: [],
                args: [],
                annotations: [],
                inputs: [
                    {
                        path: "/in/1",
                        tags: ["a:foo", "b:bar"],
                        upstreams: [],
                    },
                ],
                outputs: [],
                active: true,
            } satisfies RawPlanDetail);
            await planService.fetchById(id);
            expect(mockApiClient.get).toHaveBeenCalledWith(`/plans/${id}`);
        });
    });
});
