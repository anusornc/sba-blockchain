import type { ApiEnvelope, TraceKind, UITraceResponseData, UIOntologyData } from './types';

const configuredBaseUrl = import.meta.env.VITE_SBA_API_BASE_URL as string | undefined;

function apiUrl(path: string): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  if (!configuredBaseUrl) return normalizedPath;
  return `${configuredBaseUrl.replace(/\/$/, '')}${normalizedPath}`;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(apiUrl(path), {
    headers: {
      Accept: 'application/json',
    },
  });
  const envelope = (await response.json()) as ApiEnvelope<T>;
  if (!response.ok || !envelope.success || !envelope.data) {
    throw new Error(envelope.error || `Request failed with HTTP ${response.status}`);
  }
  return envelope.data;
}

export function fetchTrace(code: string, kind: TraceKind): Promise<UITraceResponseData> {
  const path = `/api/ui/trace/${encodeURIComponent(code)}?kind=${encodeURIComponent(kind)}`;
  return getJson<UITraceResponseData>(path);
}

export function fetchOntology(): Promise<UIOntologyData> {
  return getJson<UIOntologyData>('/api/ui/ontology');
}
