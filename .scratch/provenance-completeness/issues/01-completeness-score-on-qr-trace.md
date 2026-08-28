# 01 — Completeness score on QR and trace

**What to build:** After sample load, the QR interface and the authenticated trace for the UHT chocolate lot return a completeness score and the named missing evidence on the provenance path — not only stages and raw identifiers.

**Blocked by:** None — can start immediately.

**Status:** ready-for-human

- [x] QR for the chocolate lot is 200 and includes a completeness score in 0–1
- [x] The same response names missing evidence (empty list when the path is complete)
- [x] Authenticated trace for that lot’s entity carries the same score semantics
- [x] Existing UHT handler demo tests stay green

## Comments

Implemented on the QR/trace handler seam via `completeness-view`, wrapping the existing six-signal measure. Public JSON fields are `completeness-score` and `missing-evidence`. Full suite: 423 tests, 971 assertions, 0 failures. Missing-evidence may still be non-empty on the seeded chocolate path if a signal (e.g. time-order) is false — ticket 03 evaluates that against the rubric.
