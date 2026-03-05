import { describe, expect, it, vi } from "vitest";
import { createCloudflareCollector } from "../src/collector";

const TEST_ENV = {
  accountId: "acc-123",
  apiToken: "api-token",
  ingestUrl: "https://example.com/api/ingest",
  serviceTokenId: "svc-id",
  serviceTokenSecret: "svc-secret",
};

function makeGraphqlResponse(data: unknown): Response {
  return new Response(JSON.stringify({ data }), { status: 200 });
}

describe("collector GraphQL window limits", () => {
  it("queries worker request totals across all 5-minute buckets in the day window", async () => {
    const fetchMock = vi.fn(async () =>
      makeGraphqlResponse({
        viewer: {
          accounts: [
            {
              pagesFunctionsGroups: [{ sum: { requests: 200 } }],
              workerGroups: [{ sum: { requests: 500 } }],
            },
          ],
        },
      }),
    );
    const collector = createCloudflareCollector(TEST_ENV, fetchMock as unknown as typeof fetch);

    await collector.collectWorkerRequests("2026-03-05T12:34:56.000Z");

    const firstCall = fetchMock.mock.calls[0] as unknown[] | undefined;
    const init = firstCall?.[1] as RequestInit | undefined;
    const body = JSON.parse(String(init?.body)) as { query: string };

    expect(body.query).toContain("workersInvocationsAdaptive");
    expect(body.query).toContain("pagesFunctionsInvocationsAdaptiveGroups");
    expect(body.query).toContain("limit: 288");
    expect(body.query).not.toContain("limit: 1");
  });

  it("queries resource usage totals across all 5-minute buckets in the day window", async () => {
    const fetchMock = vi.fn(async () =>
      makeGraphqlResponse({
        viewer: {
          accounts: [
            {
              d1UsageGroups: [{ sum: { rowsRead: 10, rowsWritten: 5 } }],
              pagesFunctionsGroups: [{ sum: { requests: 30 } }],
              r2OperationGroups: [{ sum: { requests: 50 } }],
            },
          ],
        },
      }),
    );
    const collector = createCloudflareCollector(TEST_ENV, fetchMock as unknown as typeof fetch);

    await collector.collectResourceUsageTotals("2026-03-05T12:34:56.000Z");

    const firstCall = fetchMock.mock.calls[0] as unknown[] | undefined;
    const init = firstCall?.[1] as RequestInit | undefined;
    const body = JSON.parse(String(init?.body)) as { query: string };

    expect(body.query).toContain("d1AnalyticsAdaptiveGroups");
    expect(body.query).toContain("r2OperationsAdaptiveGroups");
    expect(body.query).toContain("pagesFunctionsInvocationsAdaptiveGroups");
    expect(body.query).toContain("limit: 288");
    expect(body.query).not.toContain("limit: 100");
    expect(body.query).not.toContain("limit: 1");
  });
});
