# Setup

## Requirements

- JDK 17+
- Clojure CLI
- `bash`, `curl`, and `tar`

## Steps

```bash
git clone https://github.com/anusornc/sba-blockchain.git
cd sba-blockchain
cp .env.example .env
set -a
source .env
set +a
source scripts/dev-env.bash
clojure -M:test
```

If JDK 17+ and Clojure CLI are not installed globally, install them into the
repo-local `.tools/` directory and then source `scripts/dev-env.bash`.

```bash
mkdir -p .tools/downloads .tools/jdk .tools/clojure .home
curl -L --fail -o .tools/downloads/temurin17.tar.gz \
  https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse
tar -xzf .tools/downloads/temurin17.tar.gz -C .tools/jdk --strip-components 1
curl -L --fail -o .tools/downloads/clojure-linux-install.sh \
  https://github.com/clojure/brew-install/releases/latest/download/linux-install.sh
chmod +x .tools/downloads/clojure-linux-install.sh
.tools/downloads/clojure-linux-install.sh --prefix .tools/clojure
```

Do not commit `.env`, generated credentials, local caches, or local runtime
state.
