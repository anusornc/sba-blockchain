(ns datomic-blockchain.consensus.config
  "Consensus Configuration Management
   
   Supports multi-consensus startup where users specify which
   consensus mechanism to use. All validators MUST use the same
   consensus type for network consistency."
  (:require [datomic-blockchain.consensus.protocol :as protocol]
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]))

;; ============================================================================
;; Configuration Specs
;; ============================================================================

(s/def ::consensus-type #{:poa :pos :pbft})
(s/def ::validator string?)
(s/def ::validators (s/coll-of ::validator :min-count 1))
(s/def ::stakes (s/map-of string? number?))
(s/def ::min-stake pos-int?)
(s/def ::f pos-int?)

(s/def ::poa-config
  (s/keys :req-un [::type ::validators]))

(s/def ::pos-config
  (s/keys :req-un [::type ::validators ::stakes]
          :opt-un [::min-stake]))

(s/def ::pbft-config
  (s/keys :req-un [::type ::validators ::f]))

(s/def ::consensus-config
  (s/or :poa ::poa-config
        :pos ::pos-config
        :pbft ::pbft-config))

;; ============================================================================
;; Configuration Validation
;; ============================================================================

(defn validate-consensus-config
  "Validate consensus configuration
   
   Ensures:
   1. Consensus type is valid
   2. Required fields present
   3. Validator requirements met for consensus type"
  [config]
  (let [result (s/conform ::consensus-config config)]
    (if (= result :clojure.spec.alpha/invalid)
      (let [explained (s/explain-str ::consensus-config config)]
        (log/error "Invalid consensus config:" explained)
        {:valid? false
         :error explained})
      
      (case (:type config)
        :poa {:valid? true
              :type :poa
              :validators (count (:validators config))}
        
        :pos (if (every? #(contains? (:stakes config) %) (:validators config))
               {:valid? true
                :type :pos
                :validators (count (:validators config))
                :total-stake (reduce + (vals (:stakes config)))}
               {:valid? false
                :error "All validators must have stakes defined"})
        
        :pbft (let [required (inc (* 3 (:f config)))]
                (if (>= (count (:validators config)) required)
                  {:valid? true
                   :type :pbft
                   :validators (count (:validators config))
                   :f (:f config)
                   :byzantine-faults (:f config)}
                  {:valid? false
                   :error (str "PBFT requires at least " required " validators for f=" (:f config))}))
        
        {:valid? false
         :error (str "Unknown consensus type: " (:type config))}))))

;; ============================================================================
;; Configuration from Environment/Files
;; ============================================================================

(defn load-consensus-config
  "Load consensus configuration from various sources
   
   Priority:
   1. Environment variable CONSENSUS_CONFIG (EDN string)
   2. Config file (resources/config/consensus.edn)
   3. Default config (single node PoA)"
  []
  (or
   ;; Try environment variable
   (when-let [env-config (System/getenv "CONSENSUS_CONFIG")]
     (try
       (let [config (clojure.edn/read-string env-config)]
         (log/info "Loaded consensus config from environment")
         config)
       (catch Exception e
         (log/error "Failed to parse CONSENSUS_CONFIG:" (.getMessage e))
         nil)))
   
   ;; Try config file
   (try
     (when-let [config-file (clojure.java.io/resource "config/consensus.edn")]
       (let [config (clojure.edn/read-string (slurp config-file))]
         (log/info "Loaded consensus config from file")
         config))
     (catch Exception e
       (log/warn "No consensus config file found, using defaults")
       nil))
   
   ;; Default config
   {:type :poa
    :validators ["validator-1"]}))

;; ============================================================================
;; Startup Consensus Selection
;; ============================================================================

(defn select-consensus-at-startup
  "Select and initialize consensus at system startup
   
   This is the MAIN ENTRY POINT for consensus selection.
   Users specify consensus via:
   - Environment: CONSENSUS_TYPE=poa|pos|pbft
   - Config file: {:type :poa} etc."
  []
  (log/info "Selecting consensus mechanism at startup...")
  
  (let [config (load-consensus-config)
        validation (validate-consensus-config config)]
    
    (if (:valid? validation)
      (do
        (log/info "Consensus selected:" (:type config))
        (log/info "Validators:" (count (:validators config)))
        (log/info "Config valid:" validation)
        
        ;; Create consensus manager
        (protocol/create-consensus-manager config))
      
      (do
        (log/error "Consensus config validation failed!" (:error validation))
        (throw (ex-info "Invalid consensus configuration"
                        {:config config
                         :validation validation}))))))

;; ============================================================================
;; Network Consensus Synchronization
;; ============================================================================

(defn validate-network-consensus
  "CRITICAL: Ensure all nodes in network use SAME consensus type
   
   Call this when connecting to network or adding new nodes.
   Mixing consensus types would break network consistency."
  [network-configs]
  (let [types (map #(get-in % [:consensus :type]) network-configs)
        validators (map #(get-in % [:consensus :validators]) network-configs)]
    
    (cond
      ;; Check all same type
      (not (apply = types))
      (do
        (log/error "FATAL: Network consensus type mismatch!" types)
        {:valid? false
         :error "All nodes must use the same consensus type"
         :types (set types)})
      
      ;; Check validators consistent
      (not (apply = (map set validators)))
      (do
        (log/error "FATAL: Network validator mismatch!")
        {:valid? false
         :error "All nodes must have the same validator set"
         :validator-counts (map count validators)})
      
      ;; All good
      :else
      (do
        (log/info "Network consensus validated:" (first types))
        (log/info "Validators:" (count (first validators)))
        {:valid? true
         :consensus (first types)
         :validator-count (count (first validators))
         :nodes (count network-configs)}))))

;; ============================================================================
;; Consensus Configuration Templates
;; ============================================================================

(defn poa-template
  "Template for PoA consensus config"
  [validators]
  {:type :poa
   :validators validators})

(defn pos-template
  "Template for PoS consensus config"
  [validators stakes & {:keys [min-stake] :or {min-stake 1000}}]
  {:type :pos
   :validators validators
   :stakes stakes
   :min-stake min-stake})

(defn pbft-template
  "Template for PBFT consensus config
   
   f = number of Byzantine faults to tolerate
   Requires >= 3f+1 validators"
  [validators f]
  {:type :pbft
   :validators validators
   :f f})

;; ============================================================================
;; Configuration Persistence
;; ============================================================================

(defn save-consensus-config
  "Save consensus configuration to file
   
   Use this to persist selected consensus for restarts."
  [config path]
  (spit path (pr-str config))
  (log/info "Consensus config saved to:" path))

(defn load-consensus-from-file
  "Load consensus configuration from specific file"
  [path]
  (let [config (clojure.edn/read-string (slurp path))]
    (log/info "Loaded consensus config from:" path)
    config))
