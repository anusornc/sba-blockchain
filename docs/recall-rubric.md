# Recall Rubric

The written rules that stamp a provenance path **pull** or **do-not-pull**
from named evidence defects. This is the labeling instrument for the
provenance-completeness claim: the completeness score is evaluated against
the decisions this rubric assigns (ticket 03), so the rubric is deliberately
**independent of the score** — it reads only the named defects, never the
0–1 value. Deriving labels from the score would make the evaluation circular.

The same rubric lives as data in
`src/datomic_blockchain/traceability/recall_rubric.clj` (`rubric`,
`decision-for`), which is what stamps the labeled-path fixtures
(`labeled-paths!`). If this document and the namespace disagree, the
namespace is wrong.

## Scope

The rubric answers exactly one question for a suspect lot: **pull this lot,
or do not?** It is written from the position of a recall officer who must
act on the evidence a provenance path carries — nothing else (no consumer
behavior, no purchase intent; see `CONTEXT.md`).

## Rule

**Precautionary principle: any named evidence defect pulls the lot.** A lot
is decision-ready (do-not-pull) only when its provenance path names no
missing evidence — the generating activity, the responsible agent, and a
valid chronology are all evidenced.

## Defect → decision

| Path label | Named defect | Decision | Rationale |
|---|---|---|---|
| `:complete` | none | **do-not-pull** | The provenance path names the generating activity, the responsible agent, and a valid chronology. The lot is decision-ready. |
| `:missing-activity` | `activity-present` | **pull** | There is no evidence of what was done to this lot. Pull until the step is documented. |
| `:missing-agent` | `agent-present` | **pull** | No responsible agent is attached to the generating activity; due diligence on the responsible party is impossible. Pull until responsibility is named. |
| `:missing-start-time` | `time-start-present` | **pull** | Chronology cannot be established, so the recall window cannot be ordered. Pull until the timeline is evidenced. |
| `:invalid-time-order` | `time-order-valid` | **pull** | The recorded times contradict (end before start); the evidence is internally inconsistent. Pull until the chronology is corrected. |

## Labeled paths (the fixtures)

`recall-rubric/labeled-paths!` seeds and stamps five labeled paths into a
Datomic connection — the complete UHT chocolate lot as the do-not-pull
negative control, plus one variant per defect type (injected at the
transaction level by
`traceability.sample-data/seed-labeled-path!`, defect-scoped ids so all
five coexist and are individually addressable by QR):

```
{:defect :missing-activity
 :expected-missing-evidence "activity-present"
 :anchors {:qr-code "UHT-CHOC-2024-001-QR-missing-activity" ...}
 :recall-decision {:decision :pull :rationale "..."}}
```

The completeness score's agreement with these stamps — complete above
defective, named evidence matching the injected defect, ranking consistent
with pull/do-not-pull — is the evaluation built in ticket 03.

## Why not a score threshold?

A threshold on the 0–1 score would make the label a function of the measure
being evaluated. The rubric instead names the *evidence* a recall officer
needs, so agreement between score and label is a finding about the score,
not a tautology.
