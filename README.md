# Cloudflare Billing Shield Scheduler

This directory stages the public scheduler repository for Task 10 review. It is intended to be split into its own public repository after the implementation is approved.

## Responsibilities

- Run every 5 minutes from GitHub Actions
- Collect compact Cloudflare pricing inputs with a read-only API token
- Run lightweight resource change detection every cycle
- Run full inventory reconciliation at the top of each hour
- Send one signed payload to the private `POST /api/ingest` endpoint

## Environment

- `ACCOUNT_ID`
- `CLOUDFLARE_API_TOKEN`
- `BILLING_SHIELD_INGEST_URL`
- `ACCESS_SERVICE_TOKEN_ID`
- `ACCESS_SERVICE_TOKEN_SECRET`

`CLOUDFLARE_API_TOKEN` must stay read-only. Destructive credentials belong only in the private backend repository.

## Commands

```bash
npm install
npm run test
npm run lint
npm run start
```

## Notes

- The collector posts the same ingest contract that the private backend now verifies: `202` with `{ ok: true, ingestion: ... }`.
- Lightweight detection compares the current Workers/R2 snapshot against the cached previous snapshot and surfaces additions or removals.
- Full reconciliation expands inventory coverage to Workers, R2, D1, and Pages at the top of the hour.
- The GitHub Actions workflow restores and saves `.state/inventory-snapshot.json` so diff-based change detection survives across scheduled runs.
