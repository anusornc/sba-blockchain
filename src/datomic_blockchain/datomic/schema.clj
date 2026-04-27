(ns datomic-blockchain.datomic.schema
  "Datomic schema definitions for blockchain with embedded ontology
  Integrates PROV-O ontology for traceability"
  (:require [datomic.api :as d]
            [taoensso.timbre :as log]))

;; ============================================================================
;; Blockchain Schema
;; ============================================================================

(def blockchain-schema
  "Schema for blockchain transaction and block structure"
  [{:db/ident :blockchain/transaction
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "Unique identifier for a blockchain transaction"}

   {:db/ident :blockchain/timestamp
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When the transaction was created"}

   {:db/ident :blockchain/previous-hash
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Hash of the previous transaction (chain link)"}

   {:db/ident :blockchain/hash
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Hash of this transaction"}

   {:db/ident :blockchain/data
    :db/valueType :db.type/bytes
    :db/cardinality :db.cardinality/one
    :db/doc "Transaction data (serialized)"}

   {:db/ident :blockchain/creator
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Entity that created this transaction (indexed for creator queries)"}

   {:db/ident :blockchain/signature
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Cryptographic signature of the transaction"}

   {:db/ident :blockchain/nonce
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Nonce for proof-of-work consensus"}

   {:db/ident :blockchain/cross-chain-source
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Source chain identifier for a bridged transaction"}

   {:db/ident :blockchain/cross-chain-tx
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Source-chain transaction identifier for bridged data"}])

;; ============================================================================
;; PROV-O Ontology Schema (W3C Provenance)
;; ============================================================================

(def prov-o-schema
  "Schema for W3C PROV-O ontology integration"
  [{:db/ident :prov/entity
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "PROV-O Entity: A physical, digital, conceptual, or other kind of thing"}

   {:db/ident :prov/entity-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Type/class of the entity (e.g., :product/batch, :medical/record)"}

   {:db/ident :prov/wasGeneratedBy
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "PROV-O: Activity that generated this entity (indexed for reverse lookup)"}

   {:db/ident :prov/wasDerivedFrom
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/many
    :db/index true
    :db/doc "PROV-O: Entities this was derived from (indexed for graph traversal)"}

   {:db/ident :prov/activity
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "PROV-O Activity: Something that happens over a period of time"}

   {:db/ident :prov/activity-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Type of activity (e.g., :supply-chain/transport, :processing/packaging)"}

   {:db/ident :prov/startedAtTime
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When the activity started"}

   {:db/ident :prov/endedAtTime
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When the activity ended"}

   {:db/ident :prov/wasAssociatedWith
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/many
    :db/index true
    :db/doc "PROV-O: Agent associated with this activity (indexed)"}

   {:db/ident :prov/used
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/many
    :db/index true
    :db/doc "PROV-O: Entities used by this activity (indexed)"}

   {:db/ident :prov/agent
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "PROV-O Agent: Something that bears some form of responsibility"}

   {:db/ident :prov/agent-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "PROV-O: Human-readable name of the agent"}

   {:db/ident :prov/agent-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Type of agent (e.g., :organization/supplier, :person/user)"}

   {:db/ident :prov/actedOnBehalfOf
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/doc "PROV-O: Agent this agent acted on behalf of"}])

;; ============================================================================
;; Ontology Schema
;; ============================================================================

(def ontology-schema
  "Schema for ontology management"
  [{:db/ident :ontology/id
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "Ontology identifier"}

   {:db/ident :ontology/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/index true
    :db/doc "Ontology name (e.g., 'PROV-O', 'SupplyChain')"}

   {:db/ident :ontology/namespace
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Ontology namespace URI"}

   {:db/ident :ontology/format
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "RDF format (:rdf/xml, :rdf/turtle, :rdf/n-triples)"}

   {:db/ident :ontology/data
    :db/valueType :db.type/bytes
    :db/cardinality :db.cardinality/one
    :db/doc "Ontology RDF data"}

   {:db/ident :ontology/version
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Ontology version"}

   {:db/ident :ontology/loaded-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "When ontology was loaded"}])

;; ============================================================================
;; Permission Schema
;; ============================================================================

(def permission-schema
  "Schema for permission and access control"
  [{:db/ident :permission/id
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "Permission policy identifier"}

   {:db/ident :permission/owner
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Owner of this permission policy"}

   {:db/ident :permission/resource-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Resource this permission applies to"}

   {:db/ident :permission/resource-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Type of resource (:entity, :activity, :agent)"}

   {:db/ident :permission/visibility
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Visibility level (:public, :restricted, :private)"}

   {:db/ident :permission/authorized-parties
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/many
    :db/doc "List of authorized parties for restricted access"}

   {:db/ident :permission/valid-from
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When permission becomes valid"}

   {:db/ident :permission/valid-until
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When permission expires"}

   {:db/ident :permission/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "When permission was created"}])

;; ============================================================================
;; Traceability Schema (Supply Chain)
;; ============================================================================

(def traceability-schema
  "Schema for traceability in supply chain"
  [{:db/ident :traceability/product
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Product ID being traced (UUID reference)"}

   {:db/ident :traceability/product-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Product name (human-readable)"}

   {:db/ident :traceability/batch
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Batch identifier"}

   {:db/ident :traceability/location
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Current location (GPS or named place, indexed for location queries)"}

   {:db/ident :traceability/temperature
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db/doc "Temperature reading (for cold chain)"}

   {:db/ident :traceability/certifications
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/doc "List of certifications (organic, gmo-free, etc.)"}

   {:db/ident :traceability/quantity
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one
    :db/doc "Quantity in supply chain"}

   {:db/ident :traceability/unit
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Unit of measurement (:kg, :lb, :pieces)"}])

;; ============================================================================
;; Consensus Schema
;; ============================================================================

(def consensus-schema
  "Schema for multi-consensus configuration"
  [{:db/ident :consensus/protocol
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Consensus protocol (:poa, :pos, :pbft)"}

   {:db/ident :consensus/validators
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/many
    :db/doc "List of validator agents"}

   {:db/ident :consensus/block-time
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Target time between blocks (ms)"}

   {:db/ident :consensus/block-size
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Maximum block size (bytes)"}

   ;; Runtime protocol switching attributes
   {:db/ident :consensus/available-protocols
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "EDN string of available protocols for runtime switching"}

   {:db/ident :consensus/switch-from
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Previous consensus protocol before a switch"}

   {:db/ident :consensus/switch-to
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "New consensus protocol after a switch"}

   {:db/ident :consensus/switch-by
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "User who initiated the protocol switch"}

   {:db/ident :consensus/switch-reason
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Reason for the protocol switch (audit)"}

   {:db/ident :consensus/switch-timestamp
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When the protocol switch occurred"}

   {:db/ident :consensus/block-protocol
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "The consensus protocol used to create a specific block"}

   ;; Consensus proposal status tracking
   {:db/ident :consensus/proposal-id
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc "Consensus proposal identifier for transaction tracking"}

   {:db/ident :consensus/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Current status of proposal (:pending, :approved, :rejected, :timeout, :error)"}

   {:db/ident :consensus/submitted-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When the proposal was submitted"}

   {:db/ident :consensus/updated-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "When the proposal status last changed"}

   {:db/ident :consensus/decision
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Consensus decision (:approve, :reject, :pending)"}

   {:db/ident :consensus/approve-count
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Number of approve votes recorded"}

   {:db/ident :consensus/reject-count
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Number of reject votes recorded"}

   {:db/ident :consensus/total-votes
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc "Total number of votes recorded"}

   {:db/ident :consensus/reason
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Reason for rejection/timeout/error"}])

;; ============================================================================
;; Complete Schema
;; ============================================================================

(def full-schema
  "Complete schema for blockchain with embedded ontology"
  (vec
   (concat blockchain-schema
           prov-o-schema
           ontology-schema
           permission-schema
           traceability-schema
           consensus-schema)))

(defn install-schema
  "Install schema into Datomic database"
  [conn]
  (log/info "Installing schema...")
  (try
    @(d/transact conn full-schema)
    (log/info "Schema installed successfully")
    (catch Exception e
      (log/error "Failed to install schema:" e)
      (throw e))))

(defn schema-installed?
  "Check if schema is already installed"
  [conn]
  (let [db (d/db conn)]
    (boolean (d/entity db :blockchain/transaction))))

(comment
  ;; Development REPL usage
  (def conn (datomic-blockchain.datomic.connection/connect
              (datomic-blockchain.config/load-config)))
  (install-schema conn)
  (schema-installed? conn))
