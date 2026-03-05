import { describe, expect, it, vi } from "vitest";
import { runCollector, shouldRunFullInventory } from "../src/collector";

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
    const listCurrentResources = vi.fn(async () => [
      { id: "worker:api", type: "worker" as const },
    ]);
    const reconcileInventory = vi.fn(async () => []);
    const snapshotStore = createSnapshotStore();

    await runCollector("2026-03-03T10:07:00Z", {
      cloudflare: {
        collectPricingInputs,
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
      periodStart: "2026-03-03T10:05:00.000Z",
      pricingInputs: [],
      resources: [{ id: "worker:api", type: "worker" }],
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
});
