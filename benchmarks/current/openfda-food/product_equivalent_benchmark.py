#!/usr/bin/env python3
"""Product-equivalent openFDA benchmark harness.

This harness maps the same public openFDA Food Enforcement records into
product-specific stores and measures the same four lookup families:

Q1: exact lookup by recall_number
Q2: exact lookup by deterministic entity_id
Q3: exact lookup by deterministic activity_id
Q4: count recall activities by recalling_firm

The adapters intentionally expose their storage/query paths in the manifest so
the results are not confused with older product-specific throughput benchmarks.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib import request


ENDPOINT = "https://api.fda.gov/food/enforcement.json"
PAYLOAD_PREFIX = "OPENFDA_PRODUCT_EQUIVALENT_V1\n"
QUERY_NAMES = [
    "q1_recall_number_lookup",
    "q2_entity_uuid_lookup",
    "q3_activity_uuid_lookup",
    "q4_recalling_firm_lookup",
]


@dataclass(frozen=True)
class OpenFDARecord:
    recall_number: str
    event_id: str
    recalling_firm: str
    product_description: str
    city: str
    state: str
    country: str
    distribution_pattern: str
    entity_id: str
    activity_id: str
    agent_id: str

    @property
    def location(self) -> str:
        parts = [self.city, self.state, self.country]
        location = ", ".join(part for part in parts if part)
        return location or "Unknown"


@dataclass
class Measurement:
    system: str
    query: str
    iteration: int
    status: str
    latency_ms: float


def java_name_uuid_from_bytes(value: str) -> str:
    """Match Java UUID.nameUUIDFromBytes used by the Clojure harness."""
    digest = bytearray(hashlib.md5(value.encode("utf-8")).digest())
    digest[6] &= 0x0F
    digest[6] |= 0x30
    digest[8] &= 0x3F
    digest[8] |= 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


def compact_location(source: dict[str, Any]) -> str:
    parts = [source.get("city", ""), source.get("state", ""), source.get("country", "")]
    return ", ".join(part for part in parts if part) or "Unknown"


def usable_record(source: dict[str, Any]) -> bool:
    required = ["recall_number", "event_id", "recalling_firm", "product_description"]
    return all(str(source.get(field, "")).strip() for field in required)


def normalize_record(source: dict[str, Any]) -> OpenFDARecord:
    recall_number = str(source["recall_number"])
    event_id = str(source["event_id"])
    firm = str(source["recalling_firm"])
    location = compact_location(source)
    return OpenFDARecord(
        recall_number=recall_number,
        event_id=event_id,
        recalling_firm=firm,
        product_description=str(source["product_description"]),
        city=str(source.get("city", "")),
        state=str(source.get("state", "")),
        country=str(source.get("country", "")),
        distribution_pattern=str(source.get("distribution_pattern", "")),
        entity_id=java_name_uuid_from_bytes(f"openfda-food/entity/{recall_number}"),
        activity_id=java_name_uuid_from_bytes(f"openfda-food/activity/{event_id}/{recall_number}"),
        agent_id=java_name_uuid_from_bytes(f"openfda-food/agent/{firm}/{location}"),
    )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_records(path: Path, limit: int) -> tuple[list[OpenFDARecord], dict[str, Any]]:
    raw = read_json(path)
    source_records = raw.get("records") or raw.get("results") or []
    records = [normalize_record(record) for record in source_records if usable_record(record)]
    return records[:limit], raw.get("meta", {})


def fetch_records(limit: int, out_path: Path) -> None:
    records: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    skip = 0
    remaining = limit
    while remaining > 0:
        page_limit = min(1000, remaining)
        url = f"{ENDPOINT}?limit={page_limit}&skip={skip}"
        with request.urlopen(url, timeout=60) as response:
            body = response.read().decode("utf-8")
        payload = json.loads(body)
        page = payload.get("results", [])
        if not page:
            break
        meta = payload.get("meta", meta)
        records.extend(page)
        fetched = len(page)
        skip += fetched
        remaining -= fetched
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps({"meta": meta, "records": records}, separators=(",", ":")),
        encoding="utf-8",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(sorted_values: list[float], pct: int) -> float:
    idx = max(0, int((len(sorted_values) * pct + 99) / 100) - 1)
    return sorted_values[idx]


def summarize(rows: list[Measurement], system: str) -> list[dict[str, Any]]:
    summary = []
    for query in QUERY_NAMES:
        query_rows = [row for row in rows if row.query == query]
        ok = [row.latency_ms for row in query_rows if row.status == "ok"]
        ok.sort()
        if ok:
            summary.append(
                {
                    "system": system,
                    "query": query,
                    "ok_count": len(ok),
                    "error_count": len(query_rows) - len(ok),
                    "mean_ms": sum(ok) / len(ok),
                    "p50_ms": percentile(ok, 50),
                    "p95_ms": percentile(ok, 95),
                    "p99_ms": percentile(ok, 99),
                    "min_ms": ok[0],
                    "max_ms": ok[-1],
                }
            )
        else:
            summary.append(
                {
                    "system": system,
                    "query": query,
                    "ok_count": 0,
                    "error_count": len(query_rows),
                    "mean_ms": None,
                    "p50_ms": None,
                    "p95_ms": None,
                    "p99_ms": None,
                    "min_ms": None,
                    "max_ms": None,
                }
            )
    return summary


def format_ms(value: float | None) -> str:
    return "NA" if value is None else f"{value:.3f}"


def write_raw_csv(path: Path, rows: list[Measurement]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["system", "query", "iteration", "status", "latency_ms"])
        writer.writeheader()
        for row in rows:
            data = asdict(row)
            data["latency_ms"] = format_ms(row.latency_ms)
            writer.writerow(data)


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "system",
        "query",
        "ok_count",
        "error_count",
        "mean_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "min_ms",
        "max_ms",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            formatted = dict(row)
            for field in ["mean_ms", "p50_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"]:
                formatted[field] = format_ms(row[field])
            writer.writerow(formatted)


def http_json(url: str, payload: dict[str, Any], auth: tuple[str, str] | None = None, timeout: int = 120) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=body, headers={"Content-Type": "application/json"})
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    with request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


class Neo4jAdapter:
    system = "neo4j"
    mode = "Neo4j HTTP transactional Cypher; indexed exact lookups"

    def __init__(self, url: str, user: str, password: str, database: str, run_id: str):
        self.endpoint = f"{url.rstrip('/')}/db/{database}/tx/commit"
        self.auth = (user, password)
        self.run_id = run_id

    def cypher(self, statement: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        response = http_json(
            self.endpoint,
            {"statements": [{"statement": statement, "parameters": parameters or {}}]},
            auth=self.auth,
        )
        if response.get("errors"):
            raise RuntimeError(response["errors"])
        return response

    def setup(self) -> None:
        statements = [
            "CREATE CONSTRAINT openfda_recall IF NOT EXISTS FOR (e:OpenFDAEntity) REQUIRE e.recall_number IS UNIQUE",
            "CREATE CONSTRAINT openfda_entity IF NOT EXISTS FOR (e:OpenFDAEntity) REQUIRE e.entity_id IS UNIQUE",
            "CREATE CONSTRAINT openfda_activity IF NOT EXISTS FOR (a:OpenFDAActivity) REQUIRE a.activity_id IS UNIQUE",
            "CREATE INDEX openfda_firm IF NOT EXISTS FOR (a:OpenFDAAgent) ON (a.name)",
            "MATCH (n:OpenFDABenchmark) DETACH DELETE n",
        ]
        for statement in statements:
            self.cypher(statement, {"run_id": self.run_id})

    def ingest(self, records: list[OpenFDARecord]) -> float:
        rows = [asdict(record) | {"location": record.location, "run_id": self.run_id} for record in records]
        statement = """
        UNWIND $rows AS row
        MERGE (agent:OpenFDAAgent {agent_id: row.agent_id})
          SET agent:OpenFDABenchmark,
              agent.run_id = row.run_id,
              agent.name = row.recalling_firm,
              agent.location = row.location
        MERGE (entity:OpenFDAEntity {entity_id: row.entity_id})
          SET entity:OpenFDABenchmark,
              entity.run_id = row.run_id,
              entity.recall_number = row.recall_number,
              entity.product_description = row.product_description,
              entity.distribution_pattern = row.distribution_pattern
        MERGE (activity:OpenFDAActivity {activity_id: row.activity_id})
          SET activity:OpenFDABenchmark,
              activity.run_id = row.run_id,
              activity.event_id = row.event_id,
              activity.recall_number = row.recall_number
        MERGE (activity)-[:USED]->(entity)
        MERGE (activity)-[:ASSOCIATED_WITH]->(agent)
        """
        start = time.perf_counter()
        for idx in range(0, len(rows), 250):
            self.cypher(statement, {"rows": rows[idx : idx + 250]})
        return (time.perf_counter() - start) * 1000

    def query_once(self, query: str, anchor: OpenFDARecord) -> Any:
        if query == "q1_recall_number_lookup":
            return self.cypher(
                """
                MATCH (e:OpenFDAEntity {recall_number: $value})
                WHERE e.run_id = $run_id
                RETURN e.entity_id AS value LIMIT 1
                """,
                {"value": anchor.recall_number, "run_id": self.run_id},
            )["results"][0]["data"]
        if query == "q2_entity_uuid_lookup":
            return self.cypher(
                """
                MATCH (e:OpenFDAEntity {entity_id: $value})
                WHERE e.run_id = $run_id
                RETURN e.entity_id AS value LIMIT 1
                """,
                {"value": anchor.entity_id, "run_id": self.run_id},
            )["results"][0]["data"]
        if query == "q3_activity_uuid_lookup":
            return self.cypher(
                """
                MATCH (a:OpenFDAActivity {activity_id: $value})
                WHERE a.run_id = $run_id
                RETURN a.activity_id AS value LIMIT 1
                """,
                {"value": anchor.activity_id, "run_id": self.run_id},
            )["results"][0]["data"]
        if query == "q4_recalling_firm_lookup":
            return self.cypher(
                """
                MATCH (activity:OpenFDAActivity)-[:ASSOCIATED_WITH]->(agent:OpenFDAAgent {name: $value})
                WHERE activity.run_id = $run_id
                RETURN count(activity) AS value
                """,
                {"value": anchor.recalling_firm, "run_id": self.run_id},
            )["results"][0]["data"]
        raise ValueError(query)

    def semantic_ok(self, query: str, result: Any) -> bool:
        if not result:
            return False
        value = result[0]["row"][0]
        if query == "q4_recalling_firm_lookup":
            return isinstance(value, int) and value > 0
        return bool(value)


class EthereumAdapter:
    system = "ethereum"
    mode = "Ethereum JSON-RPC transaction input storage; full block transaction scan per semantic query"

    def __init__(self, rpc_url: str, run_id: str):
        self.rpc_url = rpc_url
        self.run_id = run_id
        self.request_id = 0
        self.account: str | None = None

    def rpc(self, method: str, params: list[Any] | None = None) -> Any:
        self.request_id += 1
        response = http_json(
            self.rpc_url,
            {"jsonrpc": "2.0", "id": self.request_id, "method": method, "params": params or []},
        )
        if "error" in response:
            raise RuntimeError(response["error"])
        return response.get("result")

    def setup(self) -> None:
        accounts = self.rpc("eth_accounts")
        if not accounts:
            raise RuntimeError("Ethereum node returned no unlocked accounts")
        self.account = accounts[0]

    def ingest(self, records: list[OpenFDARecord]) -> float:
        assert self.account
        start = time.perf_counter()
        for record in records:
            payload_record = asdict(record) | {"run_id": self.run_id}
            payload = PAYLOAD_PREFIX + json.dumps(payload_record, separators=(",", ":"), sort_keys=True)
            tx_hash = self.rpc(
                "eth_sendTransaction",
                [
                    {
                        "from": self.account,
                        "to": self.account,
                        "gas": "0x2dc6c0",
                        "data": "0x" + payload.encode("utf-8").hex(),
                    }
                ],
            )
            self.wait_receipt(tx_hash)
        return (time.perf_counter() - start) * 1000

    def wait_receipt(self, tx_hash: str) -> dict[str, Any]:
        for _ in range(120):
            try:
                receipt = self.rpc("eth_getTransactionReceipt", [tx_hash])
            except RuntimeError as exc:
                if "transaction indexing is in progress" not in str(exc):
                    raise
                receipt = None
            if receipt:
                return receipt
            time.sleep(0.5)
        raise TimeoutError(f"timed out waiting for receipt: {tx_hash}")

    def iter_payloads(self) -> list[dict[str, Any]]:
        latest_hex = self.rpc("eth_blockNumber")
        latest = int(latest_hex, 16)
        records: list[dict[str, Any]] = []
        for block_num in range(latest + 1):
            block = self.rpc("eth_getBlockByNumber", [hex(block_num), True])
            if not block:
                continue
            for tx in block.get("transactions", []):
                data_hex = tx.get("input") or tx.get("data") or "0x"
                if data_hex == "0x":
                    continue
                try:
                    payload = bytes.fromhex(data_hex[2:]).decode("utf-8")
                except UnicodeDecodeError:
                    continue
                if not payload.startswith(PAYLOAD_PREFIX):
                    continue
                records.append(json.loads(payload[len(PAYLOAD_PREFIX) :]))
        return records

    def query_once(self, query: str, anchor: OpenFDARecord) -> Any:
        records = [record for record in self.iter_payloads() if record.get("run_id") == self.run_id]
        if query == "q1_recall_number_lookup":
            return next((record for record in records if record["recall_number"] == anchor.recall_number), None)
        if query == "q2_entity_uuid_lookup":
            return next((record for record in records if record["entity_id"] == anchor.entity_id), None)
        if query == "q3_activity_uuid_lookup":
            return next((record for record in records if record["activity_id"] == anchor.activity_id), None)
        if query == "q4_recalling_firm_lookup":
            return sum(1 for record in records if record["recalling_firm"] == anchor.recalling_firm)
        raise ValueError(query)

    def semantic_ok(self, query: str, result: Any) -> bool:
        if query == "q4_recalling_firm_lookup":
            return isinstance(result, int) and result > 0
        return result is not None


class FabricAdapter:
    system = "fabric"
    mode = "Hyperledger Fabric peer CLI against openFDA chaincode; composite-key exact lookups"

    def __init__(self, testnet_dir: Path, channel: str, chaincode: str):
        self.testnet_dir = testnet_dir
        self.channel = channel
        self.chaincode = chaincode
        self.env = os.environ.copy()
        pwd = str(testnet_dir)
        self.env.update(
            {
                "PATH": f"{pwd}/../bin:{self.env.get('PATH', '')}",
                "FABRIC_CFG_PATH": f"{pwd}/../config/",
                "CORE_PEER_TLS_ENABLED": "true",
                "CORE_PEER_LOCALMSPID": "Org1MSP",
                "CORE_PEER_TLS_ROOTCERT_FILE": f"{pwd}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt",
                "CORE_PEER_MSPCONFIGPATH": f"{pwd}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp",
                "CORE_PEER_ADDRESS": "localhost:7051",
            }
        )
        self.orderer_ca = f"{pwd}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem"
        self.org1_ca = f"{pwd}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"
        self.org2_ca = f"{pwd}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt"

    def setup(self) -> None:
        if not self.testnet_dir.exists():
            raise RuntimeError(f"Fabric test-network not found: {self.testnet_dir}")

    def peer(self, args: list[str], timeout: int = 180) -> str:
        result = subprocess.run(
            ["peer", "chaincode"] + args,
            cwd=self.testnet_dir,
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stdout)
        return result.stdout

    def invoke(self, function: str, args: list[str]) -> str:
        payload = json.dumps({"function": function, "Args": args}, separators=(",", ":"))
        command = [
            "invoke",
            "-o",
            "localhost:7050",
            "--ordererTLSHostnameOverride",
            "orderer.example.com",
            "--tls",
            "--cafile",
            self.orderer_ca,
            "-C",
            self.channel,
            "-n",
            self.chaincode,
            "--peerAddresses",
            "localhost:7051",
            "--tlsRootCertFiles",
            self.org1_ca,
            "--peerAddresses",
            "localhost:9051",
            "--tlsRootCertFiles",
            self.org2_ca,
            "-c",
            payload,
        ]
        retryable = [
            "chaincode registration failed",
            "context deadline exceeded",
            "could not launch chaincode",
            "connection to openfda",
        ]
        for attempt in range(1, 7):
            try:
                return self.peer(command)
            except RuntimeError as exc:
                message = str(exc)
                if attempt == 6 or not any(text in message for text in retryable):
                    raise
                time.sleep(5)
        raise RuntimeError("unreachable Fabric invoke retry state")

    def query(self, function: str, args: list[str]) -> str:
        payload = json.dumps({"Args": [function] + args}, separators=(",", ":"))
        return self.peer(["query", "-C", self.channel, "-n", self.chaincode, "-c", payload], timeout=60)

    def ingest(self, records: list[OpenFDARecord]) -> float:
        start = time.perf_counter()
        for record in records:
            output = self.invoke("PutRecord", [json.dumps(asdict(record), separators=(",", ":"), sort_keys=True)])
            if "Chaincode invoke successful" not in output:
                raise RuntimeError(output)
        return (time.perf_counter() - start) * 1000

    def query_once(self, query: str, anchor: OpenFDARecord) -> Any:
        if query == "q1_recall_number_lookup":
            return self.query("GetByRecallNumber", [anchor.recall_number])
        if query == "q2_entity_uuid_lookup":
            return self.query("GetByEntityID", [anchor.entity_id])
        if query == "q3_activity_uuid_lookup":
            return self.query("GetByActivityID", [anchor.activity_id])
        if query == "q4_recalling_firm_lookup":
            return self.query("CountByFirm", [anchor.recalling_firm])
        raise ValueError(query)

    def semantic_ok(self, query: str, result: Any) -> bool:
        if query == "q4_recalling_firm_lookup":
            try:
                return int(str(result).strip()) > 0
            except ValueError:
                return False
        return anchor_field_present(str(result))


def anchor_field_present(output: str) -> bool:
    return "entity_id" in output or "activity_id" in output or "recall_number" in output


def measure(adapter: Any, records: list[OpenFDARecord], warmup: int, reps: int) -> tuple[float, list[Measurement]]:
    adapter.setup()
    ingest_ms = adapter.ingest(records)
    anchor = records[0]
    rows: list[Measurement] = []
    for query in QUERY_NAMES:
        for _ in range(warmup):
            adapter.query_once(query, anchor)
        for idx in range(reps):
            start = time.perf_counter()
            try:
                result = adapter.query_once(query, anchor)
                status = "ok" if adapter.semantic_ok(query, result) else "semantic-error"
            except Exception as exc:  # noqa: BLE001 - record benchmark errors without hiding them.
                result = str(exc)
                status = "error"
            latency_ms = (time.perf_counter() - start) * 1000
            rows.append(Measurement(adapter.system, query, idx + 1, status, latency_ms))
    return ingest_ms, rows


def write_manifest(
    path: Path,
    args: argparse.Namespace,
    records: list[OpenFDARecord],
    meta: dict[str, Any],
    adapter: Any,
    ingest_ms: float,
    raw_rows: list[Measurement],
) -> None:
    total_errors = sum(1 for row in raw_rows if row.status != "ok")
    source_total = (meta.get("results") or {}).get("total", "unknown")
    lines = {
        "run_id": args.run_id,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "system": adapter.system,
        "adapter_mode": adapter.mode,
        "source": "openFDA Food Enforcement Reports",
        "source_endpoint": ENDPOINT,
        "source_terms": "https://open.fda.gov/terms/",
        "source_license": "https://open.fda.gov/license/ (CC0 unless otherwise noted)",
        "source_raw_json": str(args.input),
        "source_raw_json_sha256": sha256_file(args.input),
        "source_last_updated": meta.get("last_updated", "unknown"),
        "source_total_available": source_total,
        "limit": str(args.limit),
        "usable_records": str(len(records)),
        "warmup": str(args.warmup),
        "reps": str(args.reps),
        "ingest_ms": format_ms(ingest_ms),
        "git_commit": args.git_commit,
        "git_dirty": args.git_dirty,
        "selected_recall_number": records[0].recall_number,
        "selected_event_id": records[0].event_id,
        "selected_recalling_firm": records[0].recalling_firm,
        "selected_entity_id": records[0].entity_id,
        "selected_activity_id": records[0].activity_id,
        "selected_agent_id": records[0].agent_id,
        "total_errors": str(total_errors),
        "query_families": ",".join(QUERY_NAMES),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{key}={value}\n" for key, value in lines.items()), encoding="utf-8")


def build_adapter(args: argparse.Namespace) -> Any:
    if args.system == "neo4j":
        return Neo4jAdapter(args.neo4j_url, args.neo4j_user, args.neo4j_password, args.neo4j_database, args.run_id)
    if args.system == "ethereum":
        return EthereumAdapter(args.ethereum_rpc_url, args.run_id)
    if args.system == "fabric":
        return FabricAdapter(Path(args.fabric_testnet_dir), args.fabric_channel, args.fabric_chaincode)
    raise ValueError(args.system)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=["neo4j", "ethereum", "fabric"], required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--git-commit", default="unknown")
    parser.add_argument("--git-dirty", default="unknown")
    parser.add_argument("--fetch-if-missing", action="store_true")
    parser.add_argument("--neo4j-url", default="http://127.0.0.1:7474")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="openfda-benchmark")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--ethereum-rpc-url", default="http://127.0.0.1:8545")
    parser.add_argument(
        "--fabric-testnet-dir",
        default="benchmarks/practical/fabric/fabric-samples/test-network",
    )
    parser.add_argument("--fabric-channel", default="openfda-channel")
    parser.add_argument("--fabric-chaincode", default="openfda")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.fetch_if_missing and not args.input.exists():
        fetch_records(args.limit, args.input)
    records, meta = load_records(args.input, args.limit)
    if not records:
        raise RuntimeError("no usable openFDA records found")
    adapter = build_adapter(args)
    ingest_ms, raw_rows = measure(adapter, records, args.warmup, args.reps)
    summary = summarize(raw_rows, adapter.system)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_raw_csv(args.out_dir / "product_equivalent_latency_raw.csv", raw_rows)
    write_summary_csv(args.out_dir / "product_equivalent_summary.csv", summary)
    write_manifest(args.out_dir / "manifest.txt", args, records, meta, adapter, ingest_ms, raw_rows)
    print(f"{adapter.system} product-equivalent openFDA benchmark complete")
    print(f"Summary: {args.out_dir / 'product_equivalent_summary.csv'}")
    return 0 if all(row.status == "ok" for row in raw_rows) else 2


if __name__ == "__main__":
    raise SystemExit(main())
