import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

export interface SchedulerResource {
  id: string;
  status?: "removed";
  type: "d1" | "pages" | "r2" | "worker";
}

export interface WorkersPricingInput {
  includedRequests?: number;
  kind: "workers";
  requests: number;
  usdPerMillionRequests: number;
}

export interface D1PricingInput {
  kind: "d1";
  rowsRead: number;
  rowsWritten: number;
  storageGbMonths: number;
  usdPerGbMonthStored: number;
  usdPerMillionRowsRead: number;
  usdPerMillionRowsWritten: number;
}

export interface R2PricingInput {
  classAOperations: number;
  classBOperations: number;
  kind: "r2";
  storageGbMonths: number;
  usdPerGbMonthStored: number;
  usdPerMillionClassA: number;
  usdPerMillionClassB: number;
}

export interface PagesFunctionsPricingInput {
  includedRequests?: number;
  kind: "pages-functions";
  requests: number;
  usdPerMillionRequests: number;
}

export type PricingInput =
  | D1PricingInput
  | PagesFunctionsPricingInput
  | R2PricingInput
  | WorkersPricingInput;

export interface IngestPayload {
  elapsedSeconds: number;
  periodStart: string;
  pricingInputs: PricingInput[];
  resources: SchedulerResource[];
  totalSecondsInPeriod: number;
}

export interface CloudflareCollectorClient {
  collectPricingInputs(timestamp: string): Promise<PricingInput[]>;
  listCurrentResources(timestamp: string): Promise<SchedulerResource[]>;
  reconcileInventory(timestamp: string): Promise<SchedulerResource[]>;
}

export interface IngestConfig {
  serviceTokenId: string;
  serviceTokenSecret: string;
  url: string;
}

export interface RunCollectorDependencies {
  cloudflare: CloudflareCollectorClient;
  fetchFn?: typeof fetch;
  ingest: IngestConfig;
  snapshotStore?: SnapshotStore;
}

interface IngestResponseBody {
  ingestion?: {
    periodStart?: string;
  };
  ok?: boolean;
}

interface CloudflareApiResponse<T> {
  errors?: Array<{ code?: number; message?: string }>;
  result: T;
  success: boolean;
}

interface SchedulerRuntimeEnv {
  accountId: string;
  apiToken: string;
  ingestUrl: string;
  serviceTokenId: string;
  serviceTokenSecret: string;
}

export interface SnapshotStore {
  load(): Promise<SchedulerResource[]>;
  save(resources: SchedulerResource[]): Promise<void>;
}

interface CollectionWindow {
  elapsedSeconds: number;
  periodStart: string;
  totalSecondsInPeriod: number;
  windowEnd: string;
}

const DEFAULT_PRICING = {
  d1: {
    usdPerGbMonthStored: 0.75,
    usdPerMillionRowsRead: 0.001,
    usdPerMillionRowsWritten: 1,
  },
  pagesFunctions: {
    usdPerMillionRequests: 0.3,
  },
  r2: {
    usdPerGbMonthStored: 0.015,
    usdPerMillionClassA: 4.5,
    usdPerMillionClassB: 0.36,
  },
  workers: {
    includedRequests: 10_000_000,
    usdPerMillionRequests: 0.3,
  },
} as const;

function getRequiredEnv(name: string): string {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable ${name}`);
  }

  return value;
}

function normalizeTimestamp(timestamp: string): Date {
  const date = new Date(timestamp);

  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid timestamp ${timestamp}`);
  }

  return date;
}

function dedupeResources(resources: SchedulerResource[]): SchedulerResource[] {
  const deduped = new Map<string, SchedulerResource>();

  for (const resource of resources) {
    deduped.set(`${resource.type}:${resource.id}`, resource);
  }

  return [...deduped.values()];
}

function getWindowStart(timestamp: string): string {
  const date = normalizeTimestamp(timestamp);
  const flooredMinutes = Math.floor(date.getUTCMinutes() / 5) * 5;

  date.setUTCMinutes(flooredMinutes, 0, 0);

  return date.toISOString();
}

export function getCollectionWindow(timestamp: string): CollectionWindow {
  const end = normalizeTimestamp(timestamp);
  const periodStart = getWindowStart(timestamp);
  const start = new Date(periodStart);

  return {
    elapsedSeconds: Math.max(Math.round((end.getTime() - start.getTime()) / 1000), 0),
    periodStart,
    totalSecondsInPeriod: getSecondsInUtcMonth(timestamp),
    windowEnd: end.toISOString(),
  };
}

function getSecondsInUtcMonth(timestamp: string): number {
  const date = normalizeTimestamp(timestamp);
  const start = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
  const end = Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1);

  return Math.round((end - start) / 1000);
}

function unwrapArrayResult<T>(
  result: T[] | { buckets?: T[]; databases?: T[]; projects?: T[]; scripts?: T[] } | null | undefined,
): T[] {
  if (Array.isArray(result)) {
    return result;
  }

  if (!result) {
    return [];
  }

  if (Array.isArray(result.buckets)) {
    return result.buckets;
  }

  if (Array.isArray(result.databases)) {
    return result.databases;
  }

  if (Array.isArray(result.projects)) {
    return result.projects;
  }

  if (Array.isArray(result.scripts)) {
    return result.scripts;
  }

  return [];
}

function classifyR2Operation(action: string | null | undefined): "a" | "b" {
  const normalized = (action ?? "").toLowerCase();

  if (
    normalized.includes("list") ||
    normalized.includes("put") ||
    normalized.includes("create") ||
    normalized.includes("delete") ||
    normalized.includes("copy")
  ) {
    return "a";
  }

  return "b";
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  return (await response.json()) as T;
}

export function shouldRunFullInventory(timestamp: string): boolean {
  return normalizeTimestamp(timestamp).getUTCMinutes() === 0;
}

function resourceKey(resource: SchedulerResource): string {
  return `${resource.type}:${resource.id}`;
}

function toRemovedResource(resource: SchedulerResource): SchedulerResource {
  return {
    id: resource.id,
    status: "removed",
    type: resource.type,
  };
}

function diffResourceSnapshots(
  previous: SchedulerResource[],
  current: SchedulerResource[],
): SchedulerResource[] {
  const previousKeys = new Set(previous.map(resourceKey));
  const currentKeys = new Set(current.map(resourceKey));
  const changes: SchedulerResource[] = [];

  for (const resource of current) {
    if (!previousKeys.has(resourceKey(resource))) {
      changes.push(resource);
    }
  }

  for (const resource of previous) {
    if (!currentKeys.has(resourceKey(resource))) {
      changes.push(toRemovedResource(resource));
    }
  }

  return changes;
}

export function createFileSnapshotStore(filePath: string): SnapshotStore {
  return {
    load: async () => {
      try {
        const content = await readFile(filePath, "utf8");
        const parsed = JSON.parse(content) as unknown;

        if (!Array.isArray(parsed)) {
          return [];
        }

        return parsed.filter(
          (item): item is SchedulerResource =>
            Boolean(item) &&
            typeof item === "object" &&
            typeof (item as SchedulerResource).id === "string" &&
            typeof (item as SchedulerResource).type === "string",
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);

        if (message.includes("ENOENT")) {
          return [];
        }

        throw error;
      }
    },
    save: async (resources) => {
      await mkdir(path.dirname(filePath), { recursive: true });
      await writeFile(filePath, `${JSON.stringify(resources, null, 2)}\n`, "utf8");
    },
  };
}

export async function runCollector(
  timestamp: string,
  dependencies: RunCollectorDependencies,
): Promise<{ payload: IngestPayload; response: IngestResponseBody }> {
  const fetchFn = dependencies.fetchFn ?? fetch;
  const window = getCollectionWindow(timestamp);
  const pricingInputs = await dependencies.cloudflare.collectPricingInputs(timestamp);
  const previousSnapshot = dedupeResources(await dependencies.snapshotStore?.load?.() ?? []);
  const fullInventoryRun = shouldRunFullInventory(timestamp);
  const currentResources = fullInventoryRun
    ? dedupeResources(await dependencies.cloudflare.reconcileInventory(timestamp))
    : dedupeResources(await dependencies.cloudflare.listCurrentResources(timestamp));
  const resources = fullInventoryRun
    ? currentResources
    : diffResourceSnapshots(previousSnapshot, currentResources);
  const nextSnapshot = currentResources;

  if (dependencies.snapshotStore) {
    await dependencies.snapshotStore.save(nextSnapshot);
  }
  const payload: IngestPayload = {
    elapsedSeconds: window.elapsedSeconds,
    periodStart: window.periodStart,
    pricingInputs,
    resources: dedupeResources(resources),
    totalSecondsInPeriod: window.totalSecondsInPeriod,
  };

  const response = await fetchFn(dependencies.ingest.url, {
    body: JSON.stringify(payload),
    headers: {
      "CF-Access-Client-Id": dependencies.ingest.serviceTokenId,
      "CF-Access-Client-Secret": dependencies.ingest.serviceTokenSecret,
      "content-type": "application/json; charset=utf-8",
    },
    method: "POST",
  });
  const body = await parseJsonResponse<IngestResponseBody>(response);

  if (!response.ok || body.ok !== true) {
    throw new Error(
      `Ingest request failed with status ${response.status} and ok=${String(body.ok ?? false)}`,
    );
  }

  return {
    payload,
    response: body,
  };
}

export function createCloudflareCollector(
  env: SchedulerRuntimeEnv,
  fetchFn: typeof fetch = fetch,
): CloudflareCollectorClient {
  async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set("authorization", `Bearer ${env.apiToken}`);

    const response = await fetchFn(`https://api.cloudflare.com/client/v4${path}`, {
      ...init,
      headers,
    });
    const payload = await parseJsonResponse<CloudflareApiResponse<T>>(response);

    if (!response.ok || !payload.success) {
      const message =
        payload.errors?.map((error) => error.message ?? String(error.code ?? "unknown")).join(", ") ??
        `request_failed_${response.status}`;

      throw new Error(message);
    }

    return payload.result;
  }

  async function graphql<T>(query: string, variables: Record<string, unknown>): Promise<T> {
    const headers = new Headers({
      "authorization": `Bearer ${env.apiToken}`,
      "content-type": "application/json; charset=utf-8",
    });
    const response = await fetchFn("https://api.cloudflare.com/client/v4/graphql", {
      body: JSON.stringify({
        query,
        variables,
      }),
      headers,
      method: "POST",
    });
    const payload = await parseJsonResponse<{
      data?: T;
      errors?: Array<{ message: string }>;
    }>(response);

    if (!response.ok || payload.errors?.length) {
      throw new Error(payload.errors?.map((error) => error.message).join(", ") ?? "graphql_failed");
    }

    if (!payload.data) {
      throw new Error("graphql_missing_data");
    }

    return payload.data;
  }

  async function listWorkers(): Promise<SchedulerResource[]> {
    const result = await request<Array<{ id?: string; tag?: string }>>(
      `/accounts/${env.accountId}/workers/scripts`,
    );

    return unwrapArrayResult(result)
      .map((script) => script.id ?? script.tag)
      .filter((name): name is string => typeof name === "string" && name.trim() !== "")
      .map((name) => ({ id: `worker:${name}`, type: "worker" as const }));
  }

  async function listR2Buckets(): Promise<SchedulerResource[]> {
    const result = await request<Array<{ name?: string }> | { buckets?: Array<{ name?: string }> }>(
      `/accounts/${env.accountId}/r2/buckets`,
    );

    return unwrapArrayResult(result)
      .map((bucket) => bucket.name)
      .filter((name): name is string => typeof name === "string" && name.trim() !== "")
      .map((name) => ({ id: `r2:${name}`, type: "r2" as const }));
  }

  async function listD1Databases(): Promise<SchedulerResource[]> {
    const result = await request<
      Array<{ name?: string; uuid?: string }> | { databases?: Array<{ name?: string; uuid?: string }> }
    >(`/accounts/${env.accountId}/d1/database`);

    return unwrapArrayResult(result)
      .map((database) => database.name ?? database.uuid)
      .filter((name): name is string => typeof name === "string" && name.trim() !== "")
      .map((name) => ({ id: `d1:${name}`, type: "d1" as const }));
  }

  async function listPagesProjects(): Promise<SchedulerResource[]> {
    const result = await request<
      Array<{ name?: string }> | { projects?: Array<{ name?: string }> }
    >(`/accounts/${env.accountId}/pages/projects`);

    return unwrapArrayResult(result)
      .map((project) => project.name)
      .filter((name): name is string => typeof name === "string" && name.trim() !== "")
      .map((name) => ({ id: `pages:${name}`, type: "pages" as const }));
  }

  async function collectPricingInputs(timestamp: string): Promise<PricingInput[]> {
    const window = getCollectionWindow(timestamp);
    let data: {
      viewer: {
        accounts: Array<{
          d1StorageGroups?: Array<{
            dimensions?: { databaseName?: string };
            max?: { databaseSizeBytes?: number | null };
          }>;
          d1UsageGroups?: Array<{
            dimensions?: { databaseName?: string };
            sum?: { rowsRead?: number | null; rowsWritten?: number | null };
          }>;
          r2OperationGroups?: Array<{
            dimensions?: { actionType?: string | null };
            sum?: { requests?: number | null };
          }>;
          r2StorageGroups?: Array<{
            max?: { metadataSize?: number | null; payloadSize?: number | null };
          }>;
          workerGroups?: Array<{ sum?: { requests?: number | null } }>;
        }>;
      };
    };

    try {
      data = await graphql<{
        viewer: {
          accounts: Array<{
            d1StorageGroups?: Array<{
              dimensions?: { databaseName?: string };
              max?: { databaseSizeBytes?: number | null };
            }>;
            d1UsageGroups?: Array<{
              dimensions?: { databaseName?: string };
              sum?: { rowsRead?: number | null; rowsWritten?: number | null };
            }>;
            r2OperationGroups?: Array<{
              dimensions?: { actionType?: string | null };
              sum?: { requests?: number | null };
            }>;
            r2StorageGroups?: Array<{
              max?: { metadataSize?: number | null; payloadSize?: number | null };
            }>;
            workerGroups?: Array<{ sum?: { requests?: number | null } }>;
          }>;
        };
      }>(
        `
          query BillingShieldWindow($accountTag: string!, $start: Time!, $end: Time!) {
            viewer {
              accounts(filter: { accountTag: $accountTag }) {
                workerGroups: workersInvocationsAdaptive(
                  limit: 1
                  filter: { datetime_geq: $start, datetime_lt: $end }
                ) {
                  sum {
                    requests
                  }
                }
                d1UsageGroups: d1AnalyticsAdaptiveGroups(
                  filter: { datetime_geq: $start, datetime_lt: $end }
                  limit: 100
                ) {
                  dimensions {
                    databaseName
                  }
                  sum {
                    rowsRead
                    rowsWritten
                  }
                }
                d1StorageGroups: d1StorageAdaptiveGroups(limit: 100) {
                  dimensions {
                    databaseName
                  }
                  max {
                    databaseSizeBytes
                  }
                }
                r2OperationGroups: r2OperationsAdaptiveGroups(
                  filter: { datetime_geq: $start, datetime_lt: $end }
                  limit: 100
                ) {
                  dimensions {
                    actionType
                  }
                  sum {
                    requests
                  }
                }
                r2StorageGroups: r2StorageAdaptiveGroups(limit: 100) {
                  max {
                    metadataSize
                    payloadSize
                  }
                }
              }
            }
          }
        `,
        {
          accountTag: env.accountId,
          end: window.windowEnd,
          start: window.periodStart,
        },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);

      process.stderr.write(
        `[warn] GraphQL pricing collection failed; sending empty pricing inputs: ${message}\n`,
      );

      return [];
    }

    const account = data.viewer.accounts[0];

    if (!account) {
      return [];
    }

    const workerRequests =
      account.workerGroups?.reduce(
        (sum, group) => sum + Number(group.sum?.requests ?? 0),
        0,
      ) ?? 0;
    const d1ByDatabase = new Map<string, { rowsRead: number; rowsWritten: number; storageGbMonths: number }>();

    for (const group of account.d1UsageGroups ?? []) {
      const name = group.dimensions?.databaseName;

      if (!name) {
        continue;
      }

      d1ByDatabase.set(name, {
        rowsRead: Number(group.sum?.rowsRead ?? 0),
        rowsWritten: Number(group.sum?.rowsWritten ?? 0),
        storageGbMonths: 0,
      });
    }

    for (const group of account.d1StorageGroups ?? []) {
      const name = group.dimensions?.databaseName;

      if (!name) {
        continue;
      }

      const entry = d1ByDatabase.get(name) ?? {
        rowsRead: 0,
        rowsWritten: 0,
        storageGbMonths: 0,
      };
      entry.storageGbMonths = Number(group.max?.databaseSizeBytes ?? 0) / 1024 / 1024 / 1024;
      d1ByDatabase.set(name, entry);
    }

    let r2ClassA = 0;
    let r2ClassB = 0;

    for (const group of account.r2OperationGroups ?? []) {
      const requests = Number(group.sum?.requests ?? 0);

      if (classifyR2Operation(group.dimensions?.actionType) === "a") {
        r2ClassA += requests;
      } else {
        r2ClassB += requests;
      }
    }

    const r2StorageBytes = account.r2StorageGroups?.reduce((sum, group) => {
      return (
        sum + Number(group.max?.payloadSize ?? 0) + Number(group.max?.metadataSize ?? 0)
      );
    }, 0) ?? 0;

    const pricingInputs: PricingInput[] = [
      {
        includedRequests: DEFAULT_PRICING.workers.includedRequests,
        kind: "workers",
        requests: workerRequests,
        usdPerMillionRequests: DEFAULT_PRICING.workers.usdPerMillionRequests,
      },
      {
        includedRequests: 0,
        kind: "pages-functions",
        requests: 0,
        usdPerMillionRequests: DEFAULT_PRICING.pagesFunctions.usdPerMillionRequests,
      },
      {
        classAOperations: r2ClassA,
        classBOperations: r2ClassB,
        kind: "r2",
        storageGbMonths: r2StorageBytes / 1024 / 1024 / 1024,
        usdPerGbMonthStored: DEFAULT_PRICING.r2.usdPerGbMonthStored,
        usdPerMillionClassA: DEFAULT_PRICING.r2.usdPerMillionClassA,
        usdPerMillionClassB: DEFAULT_PRICING.r2.usdPerMillionClassB,
      },
      ...[...d1ByDatabase.values()].map<D1PricingInput>((database) => ({
        kind: "d1",
        rowsRead: database.rowsRead,
        rowsWritten: database.rowsWritten,
        storageGbMonths: database.storageGbMonths,
        usdPerGbMonthStored: DEFAULT_PRICING.d1.usdPerGbMonthStored,
        usdPerMillionRowsRead: DEFAULT_PRICING.d1.usdPerMillionRowsRead,
        usdPerMillionRowsWritten: DEFAULT_PRICING.d1.usdPerMillionRowsWritten,
      })),
    ];

    return pricingInputs;
  }

  return {
    collectPricingInputs,
    listCurrentResources: async () => {
      const [workers, r2] = await Promise.all([listWorkers(), listR2Buckets()]);

      return dedupeResources([...workers, ...r2]);
    },
    reconcileInventory: async () => {
      const [workers, r2, d1, pages] = await Promise.all([
        listWorkers(),
        listR2Buckets(),
        listD1Databases(),
        listPagesProjects(),
      ]);

      return dedupeResources([...workers, ...r2, ...d1, ...pages]);
    },
  };
}

function getRuntimeEnv(): SchedulerRuntimeEnv {
  return {
    accountId: getRequiredEnv("ACCOUNT_ID"),
    apiToken: getRequiredEnv("CLOUDFLARE_API_TOKEN"),
    ingestUrl: getRequiredEnv("BILLING_SHIELD_INGEST_URL"),
    serviceTokenId: getRequiredEnv("ACCESS_SERVICE_TOKEN_ID"),
    serviceTokenSecret: getRequiredEnv("ACCESS_SERVICE_TOKEN_SECRET"),
  };
}

export async function main(now = new Date().toISOString()): Promise<void> {
  const env = getRuntimeEnv();
  const collector = createCloudflareCollector(env);
  const result = await runCollector(now, {
    cloudflare: collector,
    ingest: {
      serviceTokenId: env.serviceTokenId,
      serviceTokenSecret: env.serviceTokenSecret,
      url: env.ingestUrl,
    },
    snapshotStore: createFileSnapshotStore(
      path.resolve(process.cwd(), ".state", "inventory-snapshot.json"),
    ),
  });

  process.stdout.write(
    `${JSON.stringify(
      {
        fullInventory: shouldRunFullInventory(now),
        postedPeriodStart: result.response.ingestion?.periodStart ?? result.payload.periodStart,
        resourceCount: result.payload.resources.length,
      },
      null,
      2,
    )}\n`,
  );
}

if (import.meta.url === `file://${process.argv[1]}`) {
  void main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  });
}
