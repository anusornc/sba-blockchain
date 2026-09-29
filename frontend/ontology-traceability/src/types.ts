export type TraceKind = 'qr' | 'batch';

export interface UITraceNode {
  id: string;
  label: string;
  type: 'entity' | 'activity' | 'agent' | 'location' | 'evidence' | 'identifier';
  'ontology-class': string;
  attributes: Record<string, unknown>;
}

export interface UITraceEdge {
  id: string;
  source: string;
  target: string;
  label: string;
  type: string;
  'prov-relation': string;
  attributes: Record<string, unknown>;
}

export interface UITraceGraph {
  nodes: UITraceNode[];
  edges: UITraceEdge[];
}

export interface UITraceResponseData {
  'contract-version': string;
  source: string;
  query: {
    kind: TraceKind;
    code: string;
  };
  product: {
    batch: string;
    product: string;
    'variant-name'?: string;
    flavor?: string;
    'qr-code'?: string;
  };
  stages: Array<{
    stage: string;
    activity: string;
    location?: Record<string, unknown>;
    time?: string;
  }>;
  graph: UITraceGraph;
}

export interface UIOntologyData {
  'contract-version': string;
  prefixes: Record<string, string>;
  classes: Array<{
    id: string;
    label: string;
    type: string;
    'prov-class': string;
    description: string;
  }>;
  properties: Array<{
    id: string;
    label: string;
    source: string;
    target: string;
  }>;
}

export interface ApiEnvelope<T> {
  success: boolean;
  data?: T;
  error?: string;
  details?: unknown;
}
