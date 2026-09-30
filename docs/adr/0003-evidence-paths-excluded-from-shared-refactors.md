# Benchmark and evidence paths are excluded from shared refactors

`nk_experiment` and the performance harnesses generate their own supply-chain chains instead of sharing the journey read model. That is deliberate: their outputs are registered paper evidence pinned by hash and commit in `evidence/paper-current/`, and regenerating them churns the evidence registry. Shared traceability refactors stop at the handler views; anything that would touch a benchmark path needs its own evidence-regenerating decision first.
