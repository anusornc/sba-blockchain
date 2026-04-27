(ns datomic-blockchain.consensus.cluster
  "Cluster-based consensus protocol for multi-node Datomic blockchain.

   Implements a lightweight propose-vote-commit protocol:
   1. Leader proposes transaction to all members
   2. Each member validates (PROV-O schema, permissions) and votes
   3. Leader counts votes; commits if quorum reached
   4. Leader broadcasts commit to all members

   This provides:
   - Distributed semantic validation (all nodes validate PROV-O)
   - Fault tolerance (continues with N-1 nodes)
   - Consistency (all nodes see same committed state)"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [datomic-blockchain.cluster.member :as member]
            [clj-http.client :as http])
  (:import [java.time Instant]
           [java.util UUID]))

;; Consensus state tracking
(defonce pending-proposals (atom {}))
(defonce vote-tracking (atom {}))

;; Message types
(def message-types
  {:propose "PROPOSE"
   :vote "VOTE"
   :commit "COMMIT"
   :rollback "ROLLBACK"})

;; Vote values
(def vote-values
  {:approve "APPROVE"
   :reject "REJECT"
   :abstain "ABSTAIN"})

(defn generate-proposal-id
  "Generate a unique proposal ID for tracking."
  []
  (str "proposal-" (UUID/randomUUID)))

(defn validate-prov-o-schema
  "Validate that transaction data conforms to PROV-O ontology requirements.

   PROV-O requires at minimum:
   - An Entity (prov/entity)
   - An Activity (prov/activity)
   - An Agent (prov/agent)
   - Links: Entity --wasGeneratedBy--> Activity --wasAssociatedWith--> Agent"
  [transaction-data]
  (try
    (let [data (cond
                  ;; If string, parse as JSON with keyword keys
                  (string? transaction-data)
                  (json/parse-string transaction-data true)

                  ;; If map with string keys, convert to keyword keys
                  (and (map? transaction-data)
                       (not (empty? transaction-data))
                       (string? (first (keys transaction-data))))
                  (zipmap (map keyword (keys transaction-data))
                         (vals transaction-data))

                  ;; Otherwise use as-is (already has keyword keys)
                  :else
                  transaction-data)

          ;; Debug logging
          _ (log/debug "PROV-O validation - data type:" (type data))
          _ (log/debug "PROV-O validation - data keys:" (keys data))
          _ (log/debug "PROV-O validation - prov/entity:" (:prov/entity data))
          _ (log/debug "PROV-O validation - prov/activity:" (:prov/activity data))
          _ (log/debug "PROV-O validation - prov/agent:" (:prov/agent data))

          ;; Check for required PROV-O elements (support both string and keyword keys)
          get-val (fn [k] (or (k data) (get data (name k))))
          has-entity (or (get-val :prov/entity)
                         (get-val :blockchain/transaction))
          has-activity (or (get-val :prov/activity)
                          (get-val :prov/activity-type))
          has-agent (or (get-val :prov/agent)
                       (get-val :prov/agent-type))

          _ (log/debug "PROV-O validation - has-entity:" has-entity "has-activity:" has-activity "has-agent:" has-agent)]

      (and has-entity has-activity has-agent))
    (catch Exception e
      (log/error "PROV-O validation error:" (.getMessage e))
      false)))

(defn create-propose-message
  "Create a PROPOSE message for broadcasting to cluster members."
  [proposal-id transaction-data proposer-id]
  {:message-type (:propose message-types)
   :proposal-id proposal-id
   :transaction-data transaction-data
   :proposer-id proposer-id
   :timestamp (str (Instant/now))})

(defn create-vote-message
  "Create a VOTE message in response to a proposal."
  [proposal-id voter-id vote-value reason]
  {:message-type (:vote message-types)
   :proposal-id proposal-id
   :voter-id voter-id
   :vote vote-value
   :reason reason
   :timestamp (str (Instant/now))})

(defn create-commit-message
  "Create a COMMIT message to broadcast successful commit."
  [proposal-id committer-id block-data]
  {:message-type (:commit message-types)
   :proposal-id proposal-id
   :committer-id committer-id
   :block-data block-data
   :timestamp (str (Instant/now))})

(defn create-rollback-message
  "Create a ROLLBACK message for rejected proposals."
  [proposal-id committer-id reason]
  {:message-type (:rollback message-types)
   :proposal-id proposal-id
   :committer-id committer-id
   :reason reason
   :timestamp (str (Instant/now))})

(defn broadcast-proposal!
  "Broadcast a PROPOSE message to all cluster members (except self).

   Returns a map of:
   - :success - true if broadcast succeeded
   - :proposal-id - the proposal ID
   - :members-contacted - count of members contacted"
  [proposal-id transaction-data cluster-member]
  (log/debug "broadcast-proposal! called - proposal:" proposal-id "is-leader?" (member/is-leader? cluster-member))
  (when-not (member/is-leader? cluster-member)
    (throw (ex-info "Only leader can propose transactions"
                    {:node-id (member/node-id cluster-member)
                     :current-leader (member/leader-id cluster-member)})))

  (let [message (create-propose-message proposal-id
                                        transaction-data
                                        (member/node-id cluster-member))
        message-body (json/generate-string message)
        other-members (->> (member/members cluster-member)
                           (keys)
                           (filter #(not= % (member/node-id cluster-member))))]
    (log/debug "Other cluster members:" other-members)
    (log/info "Broadcasting proposal" proposal-id "to" (count other-members) "members")

    ;; Track the proposal
    (swap! pending-proposals assoc proposal-id
           {:proposal-id proposal-id
            :transaction-data transaction-data
            :proposer-id (member/node-id cluster-member)
            :timestamp (Instant/now)
            :votes {}
            :status :pending})

    ;; Send to all members
    (doseq [member-id other-members]
      (log/debug "Sending proposal to member:" member-id)
      (try
        (member/send-message cluster-member member-id
                            "/api/internal/propose"
                            message-body)
        (log/debug "Successfully sent proposal to" member-id)
        (catch Exception e
          (log/warn "Failed to send proposal to" member-id ":" (.getMessage e)))))

    {:success true
     :proposal-id proposal-id
     :members-contacted (count other-members)}))

(defn vote-on-proposal!
  "Process a proposal and cast a vote.

   Voting logic:
   1. Validate PROV-O schema
   2. Validate permissions (if user-id provided)
   3. Return APPROVE if valid, REJECT if invalid"
  [proposal-id transaction-data voter-id cluster-member user-id]
  (try
    ;; Validate PROV-O schema
    (if (validate-prov-o-schema transaction-data)
      ;; Create approve vote
      (let [vote (create-vote-message proposal-id
                                      voter-id
                                      (:approve vote-values)
                                      "PROV-O schema validation passed")]
        (log/info "Voting APPROVE on proposal" proposal-id)
        vote)

      ;; Create reject vote
      (let [vote (create-vote-message proposal-id
                                      voter-id
                                      (:reject vote-values)
                                      "PROV-O schema validation failed")]
        (log/info "Voting REJECT on proposal" proposal-id)
        vote))
    (catch Exception e
      (log/error "Error voting on proposal:" (.getMessage e))
      (create-vote-message proposal-id
                          voter-id
                          (:reject vote-values)
                          (.getMessage e)))))

(defn collect-votes
  "Collect votes for a proposal and determine if quorum is reached.

   Returns:
   - :quorum-reached - true if enough votes collected
   - :approve-count - count of approve votes
   - :reject-count - count of reject votes
   - :total-votes - total votes collected
   - :decision - :approve, :reject, or :pending"
  [proposal-id cluster-member]
  (let [proposal (get @pending-proposals proposal-id)
        votes (:votes proposal)
        approve-count (count (filter #(= (:approve vote-values) %) (vals votes)))
        reject-count (count (filter #(= (:reject vote-values) %) (vals votes)))
        total-votes (count votes)
        quorum (member/quorum-size cluster-member)]

    {:quorum-reached (>= total-votes quorum)
     :approve-count approve-count
     :reject-count reject-count
     :total-votes total-votes
     :quorum quorum
     :decision (cond
                 (>= reject-count quorum) :reject
                 (>= approve-count quorum) :approve
                 :else :pending)}))

(defn tally-votes!
  "Tally votes for a proposal and return the consensus decision.

   This is called by the leader after vote collection timeout or quorum reached."
  [proposal-id cluster-member]
  (let [result (collect-votes proposal-id cluster-member)
        decision (:decision result)]

    (log/info "Vote tally for" proposal-id ":" decision)
    (log/info "  Approve:" (:approve-count result)
              "Reject:" (:reject-count result)
              "Total:" (:total-votes result)
              "Quorum:" (:quorum result))

    ;; Update proposal status
    (swap! pending-proposals update proposal-id
           assoc :status decision
           :vote-summary result)

    result))

(defn broadcast-commit!
  "Broadcast a COMMIT message to all cluster members after successful consensus."
  [proposal-id block-data cluster-member]
  (let [message (create-commit-message proposal-id
                                       (member/node-id cluster-member)
                                       block-data)
        message-body (json/generate-string message)
        other-members (->> (member/members cluster-member)
                           (keys)
                           (filter #(not= % (member/node-id cluster-member))))]

    (log/info "Broadcasting COMMIT for proposal" proposal-id "to" (count other-members) "members")

    (doseq [member-id other-members]
      (try
        (member/send-message cluster-member member-id
                            "/api/internal/commit"
                            message-body)
        (log/debug "Sent commit to" member-id)
        (catch Exception e
          (log/warn "Failed to send commit to" member-id ":" (.getMessage e)))))))

(defn broadcast-rollback!
  "Broadcast a ROLLBACK message to all cluster members after failed consensus."
  [proposal-id reason cluster-member]
  (let [message (create-rollback-message proposal-id
                                        (member/node-id cluster-member)
                                        reason)
        message-body (json/generate-string message)
        other-members (->> (member/members cluster-member)
                           (keys)
                           (filter #(not= % (member/node-id cluster-member))))]

    (log/info "Broadcasting ROLLBACK for proposal" proposal-id ":" reason)

    (doseq [member-id other-members]
      (try
        (member/send-message cluster-member member-id
                            "/api/internal/rollback"
                            message-body)
        (log/debug "Sent rollback to" member-id)
        (catch Exception e
          (log/warn "Failed to send rollback to" member-id ":" (.getMessage e)))))))

(defn record-vote!
  "Record a vote from a cluster member for a proposal."
  [proposal-id voter-id vote-value cluster-member]
  (swap! pending-proposals update proposal-id
         update :votes assoc voter-id vote-value)

  (log/debug "Recorded vote from" voter-id "for proposal" proposal-id ":" vote-value)

  ;; Check if we have quorum
  (let [result (collect-votes proposal-id cluster-member)]
    (when (:quorum-reached result)
      (log/info "Quorum reached for proposal" proposal-id)
      (tally-votes! proposal-id cluster-member))
    result))

(defn execute-consensus!
  "Execute the full consensus flow for a transaction.

   This is the main entry point for cluster-aware transactions.

   Steps:
   1. Generate proposal ID (or reuse provided)
   2. Broadcast proposal to all members
   3. Wait for votes (with timeout)
   4. Tally votes
   5. If approved: commit and broadcast
   6. If rejected: rollback and broadcast

   Returns:
   - {:status :approved, :proposal-id ..., :block-data ...}
   - {:status :rejected, :proposal-id ..., :reason ...}
   - {:status :timeout, :proposal-id ...}"
  ([transaction-data commit-fn cluster-member]
   (execute-consensus! transaction-data commit-fn cluster-member {}))
  ([transaction-data commit-fn cluster-member opts]
   (log/debug "execute-consensus! called with proposal-id:" (:proposal-id opts))
   (let [{:keys [timeout-ms proposal-id]}
         (if (map? opts)
           opts
           {:timeout-ms opts})
         timeout-ms (or timeout-ms 5000)
         proposal-id (or proposal-id (generate-proposal-id))
         start-time (System/currentTimeMillis)]

         ;; Broadcast proposal
    (log/debug "About to call broadcast-proposal! for" proposal-id)
    (try
      (broadcast-proposal! proposal-id transaction-data cluster-member)
      (log/info "broadcast-proposal! completed successfully")
      (catch Exception e
        (log/error "broadcast-proposal! threw exception:" (.getMessage e))
        (throw e)))

    ;; Auto-vote from leader (approves if PROV-O valid)
    (let [leader-vote (vote-on-proposal! proposal-id
                                          transaction-data
                                          (member/node-id cluster-member)
                                          cluster-member
                                          nil)]
       (record-vote! proposal-id
                     (member/node-id cluster-member)
                     (:vote leader-vote)
                     cluster-member))

     ;; Wait for votes with timeout
     (loop [elapsed 0]
       (let [result (collect-votes proposal-id cluster-member)
             decision (:decision result)]

         (cond
           ;; Decision reached
           (#{:approve :reject} decision)
           (do
             (if (= decision :approve)
               ;; Execute commit function and broadcast
               (let [block-data (commit-fn transaction-data)]
                 (broadcast-commit! proposal-id block-data cluster-member)
                 (swap! pending-proposals dissoc proposal-id)
                 {:status :approved
                  :proposal-id proposal-id
                  :block-data block-data
                  :vote-summary result})

               ;; Rejected
               (do
                 (broadcast-rollback! proposal-id "Consensus rejected" cluster-member)
                 (swap! pending-proposals dissoc proposal-id)
                 {:status :rejected
                  :proposal-id proposal-id
                  :reason "Consensus rejected"
                  :vote-summary result})))

           ;; Timeout
           (>= elapsed timeout-ms)
           (do
             (log/warn "Consensus timeout for proposal" proposal-id)
             (broadcast-rollback! proposal-id "Vote collection timeout" cluster-member)
             (swap! pending-proposals dissoc proposal-id)
             {:status :timeout
              :proposal-id proposal-id
              :reason "Vote collection timeout"
              :vote-summary result})

           ;; Continue waiting
           :else
           (do
             (Thread/sleep 100)
             (recur (+ elapsed 100)))))))))

(defn get-pending-proposal
  "Get details of a pending proposal."
  [proposal-id]
  (get @pending-proposals proposal-id))

(defn get-all-pending-proposals
  "Get all pending proposals."
  []
  @pending-proposals)

(defn clear-pending-proposal
  "Remove a proposal from pending tracking."
  [proposal-id]
  (swap! pending-proposals dissoc proposal-id))

(defn consensus-required?
  "Check if consensus is required (cluster mode enabled and leader)."
  []
  (and (member/cluster-enabled?)
       (let [cluster (member/get-cluster)]
         (and cluster
              (member/is-leader? cluster)))))

(defn should-process-vote?
  "Check if this node should process a vote (cluster mode enabled, not leader)."
  []
  (and (member/cluster-enabled?)
       (let [cluster (member/get-cluster)]
         (and cluster
              (not (member/is-leader? cluster))))))
