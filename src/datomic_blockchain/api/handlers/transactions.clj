(ns datomic-blockchain.api.handlers.transactions
  "Transaction submission and status handlers for cluster consensus"
  (:require [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.consensus.cluster :as cluster-consensus]
            [datomic-blockchain.cluster.member :as member])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Transaction Status Tracking
;; ============================================================================

(defn- record-consensus-status!
  "Persist consensus proposal status for durable transaction tracking."
  [conn proposal-id status {:keys [vote-summary reason decision]}]
  (when conn
    (let [now (Date.)
          tx-data (cond-> {:consensus/proposal-id proposal-id
                           :consensus/status status
                           :consensus/updated-at now}
                    (= status :pending) (assoc :consensus/submitted-at now)
                    decision (assoc :consensus/decision decision)
                    reason (assoc :consensus/reason reason)
                    (map? vote-summary) (assoc :consensus/approve-count (:approve-count vote-summary)
                                               :consensus/reject-count (:reject-count vote-summary)
                                               :consensus/total-votes (:total-votes vote-summary)))]
      @(d/transact conn [tx-data]))))

(defn- fetch-consensus-status
  "Fetch persisted consensus status for a proposal."
  [conn proposal-id]
  (when conn
    (d/pull (d/db conn)
            [:consensus/proposal-id
             :consensus/status
             :consensus/submitted-at
             :consensus/updated-at
             :consensus/decision
             :consensus/approve-count
             :consensus/reject-count
             :consensus/total-votes
             :consensus/reason]
            [:consensus/proposal-id proposal-id])))

;; ============================================================================
;; Transaction Handlers
;; ============================================================================

(defn handle-submit-transaction
  "Submit a transaction for cluster consensus.
   Returns a transaction ID (proposal-id) for status polling.
   This endpoint enables asynchronous transaction submission for benchmarking."
  [request connection]
  (common/with-error-handling "Submit transaction"
    (if-not (member/cluster-enabled?)
      (common/error "Cluster mode not enabled" 503)

      ;; Parse body from request
      (let [body-params (common/extract-body-params request)
            body-params-kw (common/keywordize-body-params body-params)
            entity-id (:entity-id body-params-kw)
            entity-type (:entity-type body-params-kw)
            activity-type (:activity-type body-params-kw)
            agent-id (:agent-id body-params-kw)
            cluster-member (member/get-cluster)]

        ;; Validate required fields
        (if-not (and entity-id entity-type)
          (common/validation-error "Missing required fields" {:required [:entity-id :entity-type]})

          ;; Continue with transaction processing
          (let [tx-data {:prov/entity (UUID/fromString entity-id)
                         :prov/entity-type (keyword entity-type)
                         :prov/activity (UUID/randomUUID)
                         :prov/activity-type (or (keyword activity-type) :prov/create)
                         :prov/agent (or (when agent-id (UUID/fromString agent-id))
                                         (UUID/randomUUID))
                         :prov/agent-type :prov/organization
                         :prov/startedAtTime (java.util.Date.)
                         :prov/endedAtTime (java.util.Date.)}

                ;; Generate proposal ID and start consensus asynchronously
                proposal-id (cluster-consensus/generate-proposal-id)
                conn connection

                ;; Persist initial status
                _ (record-consensus-status! conn proposal-id :pending {})

                ;; Submit to consensus in background
                _ (future
                    (try
                      (let [result (cluster-consensus/execute-consensus!
                                    tx-data
                                    (fn [data]
                                      ;; Commit function: transact to Datomic
                                      (when conn
                                        @(d/transact conn
                                                     [{:db/id "temp"
                                                       :prov/entity (:prov/entity data)
                                                       :prov/entity-type (:prov/entity-type data)
                                                       :prov/activity (:prov/activity data)
                                                       :prov/activity-type (:prov/activity-type data)}])
                                        {:block-id (UUID/randomUUID)}))
                                    cluster-member
                                    {:timeout-ms 10000
                                     :proposal-id proposal-id})
                            vote-summary (:vote-summary result)
                            decision (:decision vote-summary)
                            reason (:reason result)]
                        (record-consensus-status! conn proposal-id (:status result)
                                                  {:vote-summary vote-summary
                                                   :decision decision
                                                   :reason reason}))
                      (catch Exception e
                        (log/error "Background consensus error:" (.getMessage e))
                        (record-consensus-status! conn proposal-id :error
                                                  {:reason (.getMessage e)}))))]

            (common/success
             {:transaction-id proposal-id
              :status :submitted
              :message "Transaction submitted for consensus"
              :timestamp (java.util.Date.)})))))))

(defn handle-transaction-status
  "Get the status of a submitted transaction.
   Polling endpoint for end-to-end transaction tracking.
   Returns: pending, approved, rejected, timeout, or not-found."
  [request connection]
  (let [transaction-id (get-in request [:params :id])]
    (log/info "Transaction status request:" transaction-id)
    (try
      (if-let [proposal (cluster-consensus/get-pending-proposal transaction-id)]
        ;; Proposal exists and is being processed
        (let [status (:status proposal)
              vote-summary (:vote-summary proposal)]
          (common/success
           {:transaction-id transaction-id
            :status (name status)
            :submitted-at (:timestamp proposal)
            :vote-summary (when vote-summary
                            {:approve-count (:approve-count vote-summary)
                             :reject-count (:reject-count vote-summary)
                             :total-votes (:total-votes vote-summary)
                             :decision (name (:decision vote-summary))})}))

        ;; Check persisted status
        (if-let [persisted (fetch-consensus-status connection transaction-id)]
          (common/success
           {:transaction-id transaction-id
            :status (name (:consensus/status persisted))
            :submitted-at (:consensus/submitted-at persisted)
            :updated-at (:consensus/updated-at persisted)
            :reason (:consensus/reason persisted)
            :vote-summary (when (:consensus/total-votes persisted)
                            {:approve-count (:consensus/approve-count persisted)
                             :reject-count (:consensus/reject-count persisted)
                             :total-votes (:consensus/total-votes persisted)
                             :decision (when (:consensus/decision persisted)
                                         (name (:consensus/decision persisted)))})})

          ;; Proposal not found
          (common/success
           {:transaction-id transaction-id
            :status :not-found
            :message "Transaction not found in pending proposals (may have completed)"})))

      (catch Exception e
        (log/error "Transaction status error:" (.getMessage e))
        (common/error (.getMessage e) 500)))))

(defn handle-list-pending-transactions
  "List all pending/active transactions.
   Useful for monitoring cluster consensus state."
  [request _connection]
  (log/info "List pending transactions request")
  (try
    (let [pending (cluster-consensus/get-all-pending-proposals)]
      (common/success
       {:count (count pending)
        :transactions (vec (for [[id proposal] pending]
                             {:transaction-id id
                              :status (name (:status proposal))
                              :submitted-at (:timestamp proposal)
                              :proposer-id (:proposer-id proposal)}))}))
    (catch Exception e
      (log/error "List pending transactions error:" (.getMessage e))
      (common/error (.getMessage e) 500))))
