import { useEffect, useRef } from 'react';
import cytoscape, { type Core, type EventObject } from 'cytoscape';
import type { UITraceGraph, UITraceNode } from './types';

interface TraceGraphProps {
  graph: UITraceGraph;
  selectedNodeId: string | null;
  onSelectNode: (node: UITraceNode | null) => void;
}

const nodeColors: Record<string, string> = {
  entity: '#0b6e4f',
  activity: '#c76f00',
  agent: '#1f5aa6',
  location: '#7a4d00',
  evidence: '#5a4fcf',
  identifier: '#2f4858',
};

export function TraceGraph({ graph, selectedNodeId, onSelectNode }: TraceGraphProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const cy = cytoscape({
      container: containerRef.current,
      elements: [
        ...graph.nodes.map((node) => ({
          data: {
            id: node.id,
            label: node.label,
            type: node.type,
          },
        })),
        ...graph.edges.map((edge) => ({
          data: {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            label: edge.label,
          },
        })),
      ],
      style: [
        {
          selector: 'node',
          style: {
            'background-color': (ele) => nodeColors[String(ele.data('type'))] || '#334155',
            color: '#f8fafc',
            label: 'data(label)',
            'font-size': '11px',
            'text-wrap': 'wrap',
            'text-max-width': '120px',
            'text-valign': 'center',
            'text-halign': 'center',
            width: '88px',
            height: '88px',
            'border-width': '2px',
            'border-color': '#f8fafc',
          },
        },
        {
          selector: 'edge',
          style: {
            width: '2px',
            'line-color': '#94a3b8',
            'target-arrow-color': '#94a3b8',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            label: 'data(label)',
            'font-size': '9px',
            color: '#334155',
            'text-background-color': '#fff7ed',
            'text-background-opacity': 0.9,
            'text-background-padding': '3px',
          },
        },
        {
          selector: '.selected',
          style: {
            'border-width': 6,
            'border-color': '#facc15',
            'overlay-opacity': 0.15,
            'overlay-color': '#facc15',
          },
        },
      ],
      layout: {
        name: 'breadthfirst',
        directed: true,
        spacingFactor: 1.45,
        padding: 42,
      },
      wheelSensitivity: 0.2,
    });

    cy.on('tap', 'node', (event: EventObject) => {
      const id = String(event.target.data('id'));
      onSelectNode(graph.nodes.find((node) => node.id === id) ?? null);
    });

    cy.on('tap', (event: EventObject) => {
      if (event.target === cy) onSelectNode(null);
    });

    cyRef.current = cy;
    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [graph, onSelectNode]);

  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.nodes().removeClass('selected');
    if (selectedNodeId) {
      cy.getElementById(selectedNodeId).addClass('selected');
    }
  }, [selectedNodeId]);

  return <div ref={containerRef} className="trace-graph" aria-label="Traceability graph" />;
}
