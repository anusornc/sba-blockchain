# Security

- Never commit real JWT secrets, passwords, API tokens, private keys, keystores,
  generated Fabric crypto material, or Datomic binaries.
- Use `.env.example` as a template and keep `.env` local.
- Public releases must be produced from a clean-room export allowlist, not by
  pushing private repository history.
- Before publishing, scan the export directory for sensitive content.
