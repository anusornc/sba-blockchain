# Semantic Blockchain Architecture

One glossary for the whole project: the domain language of the claimed
work and of the code that implements it. Terms from the public companion
(research claims, evaluation) and the implementation (trace views, the
Ledger) live side by side; when a public term and an implementation term
describe different things, both are kept and kept apart.

## Language

### Domain

**Ledger**:
The immutable Datomic transaction log that serves as the blockchain ledger.
_Avoid_: database, chain, store

**Provenance path**:
The chain of entities, activities, and agents that explains how a product record came to be.
_Avoid_: blockchain, transaction history, event log (unless you mean a GS1-style event list without PROV relations)

**Journey**:
The four-stage supply-chain sequence a batch travels: farm → manufacturing → logistics → retail.
_Avoid_: supply-chain path, history

**Stage**:
One variant-aware step within a Journey, anchored by one activity and its location.
_Avoid_: step, phase, event

**Variant**:
A product flavor family of UHT batches (chocolate, plain, strawberry, raw-milk), identified by batch-id prefix.
_Avoid_: flavor, product type

**Deterministic Dataset**:
The canonical EDN dataset of agents, products, and activities that seeds the Ledger and backs the Trace View.
_Avoid_: sample data, demo data, fixtures

**Integrity**:
Evidence that a record has not been altered after it was committed.
_Avoid_: trust, completeness, confidence

### Evidence and evaluation

**Completeness**:
Whether a provenance path has enough evidence to support a decision (recall, due diligence, release).
_Avoid_: integrity, "on-chain", traced

**Completeness score**:
The weighted 0–1 measure already implemented for a provenance path, from these signals: entity present, entity type, activity present, agent present, start time present, time-order valid. Missing signals are named in the result.
_Avoid_: confidence, trust, trust level, a new unpublished formula

**Decision-readiness**:
Whether a completeness score is high enough that a named actor should act (for this paper: a recall decision, not a shopper purchase).
_Avoid_: "traced", "verified", "on-chain"

**Recall decision**:
Pull this lot, or do not. That is the only action the completeness score is evaluated against.
_Avoid_: buy, scan, consumer trust

**Recall rubric**:
The written rules that stamp a provenance path as pull or do-not-pull from named evidence defects (missing activity, missing agent, invalid time-order).
_Avoid_: expert opinion, consumer survey

**Labeled path**:
A provenance path plus the recall decision the rubric assigns to it.
_Avoid_: test case, sample only

**NK experiment**:
A synthetic-chain generator that varies component count (N) and interdependency (K) to make provenance paths easier or harder. It is the evaluation method, not the paper's contribution.
_Avoid_: NK model as the result, fitness landscape paper

### Interfaces

**QR interface**:
A public Ledger-backed view of a provenance path plus completeness score. It is how a person sees the result, not the scientific outcome.
_Avoid_: consumer study, shopper experiment

**Trace View**:
The versioned, read-only Deterministic-Dataset-backed projection of a batch's Journey served to the traceability frontend.
_Avoid_: trace result, trace graph, UI contract

**Paper companion**:
A public-safe snapshot of source, tests, sample data, and harnesses sufficient to reproduce one claimed research result.
_Avoid_: production platform, product, L1, blockchain network
