import { FormEvent, useEffect, useState } from 'react';
import { fetchOntology, fetchTrace } from './api';
import { TraceGraph } from './TraceGraph';
import type { TraceKind, UITraceNode, UITraceResponseData, UIOntologyData } from './types';

const sampleCodes = [
  { label: 'Chocolate UHT QR', kind: 'qr' as const, code: 'UHT-CHOC-2024-001-QR' },
  { label: 'Plain UHT Batch', kind: 'batch' as const, code: 'UHT-PLAIN-CM-2024-001' },
  { label: 'Strawberry UHT QR', kind: 'qr' as const, code: 'UHT-STRAW-2024-001-QR' },
];

function formatValue(value: unknown): string {
  if (value == null) return '-';
  if (typeof value === 'string') return value;
  return JSON.stringify(value, null, 2);
}

function Inspector({ node }: { node: UITraceNode | null }) {
  if (!node) {
    return (
      <aside className="panel inspector empty">
        <span className="eyebrow">Inspector</span>
        <h2>Select a node</h2>
        <p>Click any graph node to inspect its ontology class and trace attributes.</p>
      </aside>
    );
  }

  return (
    <aside className="panel inspector">
      <span className="eyebrow">{node.type}</span>
      <h2>{node.label}</h2>
      <p className="ontology-class">{node['ontology-class']}</p>
      <dl>
        {Object.entries(node.attributes).map(([key, value]) => (
          <div key={key}>
            <dt>{key}</dt>
            <dd>{formatValue(value)}</dd>
          </div>
        ))}
      </dl>
    </aside>
  );
}

function OntologyLegend({ ontology }: { ontology: UIOntologyData | null }) {
  return (
    <section className="panel legend">
      <span className="eyebrow">Ontology Vocabulary</span>
      <h2>SBA / PROV-O View</h2>
      {!ontology ? (
        <p>Loading ontology legend...</p>
      ) : (
        <>
          <div className="legend-grid">
            {ontology.classes.map((item) => (
              <article key={item.id}>
                <strong>{item.label}</strong>
                <small>{item['prov-class']}</small>
              </article>
            ))}
          </div>
          <p className="contract">Contract {ontology['contract-version']}</p>
        </>
      )}
    </section>
  );
}

export default function App() {
  const [code, setCode] = useState(sampleCodes[0].code);
  const [kind, setKind] = useState<TraceKind>(sampleCodes[0].kind);
  const [trace, setTrace] = useState<UITraceResponseData | null>(null);
  const [ontology, setOntology] = useState<UIOntologyData | null>(null);
  const [selectedNode, setSelectedNode] = useState<UITraceNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadTrace(nextCode = code, nextKind = kind) {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchTrace(nextCode.trim(), nextKind);
      setTrace(data);
      setSelectedNode(data.graph.nodes[0] ?? null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load trace');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchOntology().then(setOntology).catch((err: unknown) => {
      setError(err instanceof Error ? err.message : 'Failed to load ontology');
    });
    void loadTrace(sampleCodes[0].code, sampleCodes[0].kind);
    // Initial sample load only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function submit(event: FormEvent) {
    event.preventDefault();
    void loadTrace();
  }

  function useSample(sample: (typeof sampleCodes)[number]) {
    setCode(sample.code);
    setKind(sample.kind);
    void loadTrace(sample.code, sample.kind);
  }

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <span className="eyebrow">Semantic Blockchain Artifact</span>
          <h1>Ontology Traceability Console</h1>
          <p>
            Read-only visual inspection for SBA supply-chain traces. The graph is loaded
            from the stable `/api/ui/trace` contract, not from hardcoded benchmark output.
          </p>
        </div>
        <form className="trace-form" onSubmit={submit}>
          <label>
            Trace code
            <input value={code} onChange={(event) => setCode(event.target.value)} />
          </label>
          <label>
            Kind
            <select value={kind} onChange={(event) => setKind(event.target.value as TraceKind)}>
              <option value="qr">QR code</option>
              <option value="batch">Batch ID</option>
            </select>
          </label>
          <button disabled={loading || !code.trim()}>{loading ? 'Loading...' : 'Trace'}</button>
        </form>
      </section>

      <div className="sample-row">
        {sampleCodes.map((sample) => (
          <button key={sample.code} type="button" onClick={() => useSample(sample)}>
            {sample.label}
          </button>
        ))}
      </div>

      {error && <div className="error-banner">{error}</div>}

      {trace && (
        <section className="summary-grid">
          <article>
            <span className="eyebrow">Product</span>
            <strong>{trace.product.product}</strong>
            <small>{trace.product.batch}</small>
          </article>
          <article>
            <span className="eyebrow">Trace Source</span>
            <strong>{trace.source}</strong>
            <small>Contract {trace['contract-version']}</small>
          </article>
          <article>
            <span className="eyebrow">Graph</span>
            <strong>{trace.graph.nodes.length} nodes</strong>
            <small>{trace.graph.edges.length} edges</small>
          </article>
        </section>
      )}

      <section className="workspace">
        <div className="panel graph-panel">
          <div className="panel-heading">
            <div>
              <span className="eyebrow">Provenance Graph</span>
              <h2>{trace ? trace.product.product : 'No trace loaded'}</h2>
            </div>
          </div>
          {trace ? (
            <TraceGraph
              graph={trace.graph}
              selectedNodeId={selectedNode?.id ?? null}
              onSelectNode={setSelectedNode}
            />
          ) : (
            <div className="trace-graph placeholder">Load a trace to render the graph.</div>
          )}
        </div>
        <Inspector node={selectedNode} />
      </section>

      <OntologyLegend ontology={ontology} />
    </main>
  );
}
