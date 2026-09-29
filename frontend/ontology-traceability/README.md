# SBA Ontology Traceability Frontend

Read-only React/Vite frontend for inspecting SBA traceability graphs through the stable UI API contracts.

## Scope

- Uses `GET /api/ui/trace/{code}?kind=qr|batch`.
- Uses `GET /api/ui/ontology` for legend/vocabulary metadata.
- Does not include OAuth, Azure OpenAI, Microsoft Fabric write APIs, ontology write-back, or local token persistence.
- Keeps the existing ClojureScript frontend unchanged.
- Provides visualization/inspection evidence only; it is not a benchmark harness and must not be cited as throughput or latency evidence.

## Run

From this directory:

```bash
npm install
npm run dev
```

By default, Vite proxies `/api` to `http://localhost:3000`.

To point directly to another SBA backend:

```bash
VITE_SBA_API_BASE_URL=http://localhost:3000 npm run dev
```

## Verify

```bash
npm test
npm run build
```

Browser smoke test from the repository root requires the backend and frontend preview to be running:

```bash
env DATOMIC_PROFILE=mem ./scripts/run-backend.sh
cd frontend/ontology-traceability
VITE_SBA_API_BASE_URL=http://127.0.0.1:3000 npm run build
npx vite preview --host 127.0.0.1 --port 5174
cd ../e2e
BASE_URL=http://127.0.0.1:5174 npx playwright test tests/ontology-traceability.spec.ts --project=chromium
BASE_URL=http://127.0.0.1:5174 npm run screenshot:ontology
```

Known MVP limitation: the initial production bundle includes Cytoscape in the main chunk and may exceed Vite's default 500 kB warning threshold. This is acceptable for the first isolated prototype and should be code-split before public release if the frontend grows.
