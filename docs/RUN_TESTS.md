# Run Tests

```bash
source scripts/dev-env.bash
clojure -M:test
```

Focused namespace example:

```bash
source scripts/dev-env.bash
clojure -M:test --focus datomic-blockchain.bridge.core-test
```
