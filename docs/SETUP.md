# Setup

## Requirements

- JDK 17+
- Clojure CLI
- Datomic dependency supplied by the user

## Steps

```bash
git clone https://github.com/anusornc/sba-blockchain.git
cd sba-blockchain
cp .env.example .env
clj -M:test
```

Do not commit `.env`, Datomic binaries, generated credentials, or local runtime
state.
