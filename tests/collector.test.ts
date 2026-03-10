import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { createFileSnapshotStore, runCollector, shouldRunFullInventory } from "../src/collector";

function getRequestInit(fetchMock: ReturnType<typeof vi.fn>): RequestInit {
  const call = fetchMock.mock.calls[0] as [RequestInfo | URL, RequestInit?] | undefined;

  return call?.[1] ?? {};
}

function createSnapshotStore(initial: Array<{ id: string; type: "d1" | "pages" | "r2" | "worker" }> = []) {
  let snapshot = [...initial];

  return {
    load: vi.fn(async () => [...snapshot]),
    save: vi.fn(async (nextSnapshot: Array<{ id: string; type: "d1" | "pages" | "r2" | "worker" }>) => {
      snapshot = [...nextSnapshot];
    }),
  };
}

describe("scheduler cadence", () => {
  it("treats an empty snapshot cache file as an empty inventory", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "collector-cache-empty-"));
    const snapshotFile = path.join(directory, "inventory-snapshot.json");
    await writeFile(snapshotFile, "", "utf8");
    const store = createFileSnapshotStore(snapshotFile);

    await expect(store.load()).resolves.toEqual([]);
  });

  it("treats a truncated snapshot cache file as an empty inventory", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "collector-cache-truncated-"));
    const snapshotFile = path.join(directory, "inventory-snapshot.json");
    await writeFile(snapshotFile, "{", "utf8");
    const store = createFileSnapshotStore(snapshotFile);

    await expect(store.load()).resolves.toEqual([]);
  });

  it("runs full inventory only at the top of the hour", () => {
    expect(shouldRunFullInventory("2026-03-03T10:00:00Z")).toBe(true);
    expect(shouldRunFullInventory("2026-03-03T10:05:00Z")).toBe(false);
  });

  it("derives elapsedSeconds from the actual delayed run window", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          ingestion: {
            periodStart: "2026-03-03T10:05:00.000Z",
          },
          ok: true,
        }),
        { status: 202 },
      );
    });
    const collectPricingInputs = vi.fn(async () => []);
    const collectResourceUsageTotals = vi.fn(async () => ({
      d1RowsUsed: 4200,
      pagesFunctionsRequests: 265,
      r2OperationsUsed: 18,
    }));
    const collectWorkerRequests = vi.fn(async () => 702);
    const listCurrentResources = vi.fn(async () => [
      { id: "worker:api", type: "worker" as const },
    ]);
    const reconcileInventory = vi.fn(async () => []);
    const snapshotStore = createSnapshotStore();

    await runCollector("2026-03-03T10:07:00Z", {
      cloudflare: {
        collectPricingInputs,
        collectResourceUsageTotals,
        collectWorkerRequests,
        listCurrentResources,
        reconcileInventory,
      },
      fetchFn: fetchMock,
      ingest: {
        serviceTokenId: "token-id",
        serviceTokenSecret: "token-secret",
        url: "https://private.example.com/api/ingest",
      },
      snapshotStore,
    });

    expect(collectPricingInputs).toHaveBeenCalledOnce();
    expect(collectResourceUsageTotals).toHaveBeenCalledOnce();
    expect(collectWorkerRequests).toHaveBeenCalledOnce();
    expect(listCurrentResources).toHaveBeenCalledOnce();
    expect(reconcileInventory).not.toHaveBeenCalled();
    expect(fetchMock).toHaveBeenCalledOnce();
    const requestInit = getRequestInit(fetchMock);

    expect(requestInit).toMatchObject({
      headers: {
        "CF-Access-Client-Id": "token-id",
        "CF-Access-Client-Secret": "token-secret",
      },
      method: "POST",
    });
    expect(JSON.parse(String(requestInit.body))).toMatchObject({
      elapsedSeconds: 120,
      d1RowsUsed: 4200,
      pagesFunctionsRequests: 265,
      periodStart: "2026-03-03T10:05:00.000Z",
      pricingInputs: [],
      r2OperationsUsed: 18,
      resources: [{ id: "worker:api", type: "worker" }],
      workerBilledUsd: 0,
      workerRequests: 702,
    });
    expect(snapshotStore.save).toHaveBeenCalledWith([
      { id: "worker:api", type: "worker" },
    ]);
  });

  it("runs full inventory reconciliation at the top of the hour", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          ingestion: {
            periodStart: "2026-03-03T11:00:00.000Z",
          },
          ok: true,
        }),
        { status: 202 },
      );
    });
    const listCurrentResources = vi.fn(async () => [
      { id: "worker:delta", type: "worker" as const },
    ]);
    const reconcileInventory = vi.fn(async () => [
      { id: "worker:api", type: "worker" as const },
      { id: "r2:assets", type: "r2" as const },
    ]);

    await runCollector("2026-03-03T11:00:00Z", {
      cloudflare: {
        collectPricingInputs: vi.fn(async () => []),
        collectResourceUsageTotals: vi.fn(async () => ({
          d1RowsUsed: 3200,
          pagesFunctionsRequests: 111,
          r2OperationsUsed: 9,
        })),
        collectWorkerRequests: vi.fn(async () => 550),
        listCurrentResources,
        reconcileInventory,
      },
      fetchFn: fetchMock,
      ingest: {
        serviceTokenId: "token-id",
        serviceTokenSecret: "token-secret",
        url: "https://private.example.com/api/ingest",
      },
    });

    expect(listCurrentResources).not.toHaveBeenCalled();
    expect(reconcileInventory).toHaveBeenCalledOnce();
    const requestInit = getRequestInit(fetchMock);

    expect(JSON.parse(String(requestInit.body))).toMatchObject({
      resources: [
        { id: "worker:api", type: "worker" },
        { id: "r2:assets", type: "r2" },
      ],
      d1RowsUsed: 3200,
      pagesFunctionsRequests: 111,
      r2OperationsUsed: 9,
      workerBilledUsd: 0,
      workerRequests: 550,
    });
  });

  it("surfaces removed lightweight resources instead of silently dropping them", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          ingestion: {
            periodStart: "2026-03-03T10:05:00.000Z",
          },
          ok: true,
        }),
        { status: 202 },
      );
    });
    const snapshotStore = createSnapshotStore([
      { id: "worker:api", type: "worker" },
      { id: "r2:assets", type: "r2" },
    ]);

    await runCollector("2026-03-03T10:07:00Z", {
      cloudflare: {
        collectPricingInputs: vi.fn(async () => []),
        collectResourceUsageTotals: vi.fn(async () => ({
          d1RowsUsed: 0,
          pagesFunctionsRequests: 0,
          r2OperationsUsed: 0,
        })),
        collectWorkerRequests: vi.fn(async () => 0),
        listCurrentResources: vi.fn(async () => [{ id: "r2:assets", type: "r2" as const }]),
        reconcileInventory: vi.fn(async () => []),
      },
      fetchFn: fetchMock,
      ingest: {
        serviceTokenId: "token-id",
        serviceTokenSecret: "token-secret",
        url: "https://private.example.com/api/ingest",
      },
      snapshotStore,
    });

    const requestInit = getRequestInit(fetchMock);

    expect(JSON.parse(String(requestInit.body))).toMatchObject({
      resources: [{ id: "worker:api", status: "removed", type: "worker" }],
    });
    expect(snapshotStore.save).toHaveBeenCalledWith([
      { id: "r2:assets", type: "r2" },
    ]);
  });

  it("accepts a successful ingest response with an empty body", async () => {
    const fetchMock = vi.fn(async () => new Response(null, { status: 202 }));

    const result = await runCollector("2026-03-03T10:07:00Z", {
      cloudflare: {
        collectPricingInputs: vi.fn(async () => []),
        collectResourceUsageTotals: vi.fn(async () => ({
          d1RowsUsed: 0,
          pagesFunctionsRequests: 0,
          r2OperationsUsed: 0,
        })),
        collectWorkerRequests: vi.fn(async () => 0),
        listCurrentResources: vi.fn(async () => []),
        reconcileInventory: vi.fn(async () => []),
      },
      fetchFn: fetchMock,
      ingest: {
        serviceTokenId: "token-id",
        serviceTokenSecret: "token-secret",
        url: "https://private.example.com/api/ingest",
      },
    });

    expect(result.response.ok).toBe(true);
  });
});
