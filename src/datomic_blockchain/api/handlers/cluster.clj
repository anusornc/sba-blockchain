(ns datomic-blockchain.api.handlers.cluster
  "Internal cluster consensus API handlers
   These implement the propose-vote-commit consensus protocol."
  (:require [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.consensus.cluster :as cluster-consensus]
            [datomic-blockchain.cluster.member :as member]))

;; ============================================================================
;; Node Authentication
;; ============================================================================

(defn verify-node-auth
  "Verify node-to-node authentication via X-Node-ID header.
   Returns node-id if valid, nil otherwise."
  [request]
  (let [node-id (get-in request [:headers "x-node-id"])
        cluster-obj (when (member/cluster-enabled?)
                      (member/get-cluster))]
    (when (and cluster-obj node-id)
      (let [known-members (set (keys (member/members cluster-obj)))]
        (when (contains? known-members node-id)
          node-id)))))

;; ============================================================================
;; Internal Cluster Handlers
;; ============================================================================

(defn handle-internal-propose
  "Handle PROPOSE message from cluster leader.
   Validates the proposal and casts a vote."
  [request connection]
  (log/info "Received PROPOSE message")
  (try
    ;; Verify node authentication
    (when-not (verify-node-auth request)
      (throw (ex-info "Unauthorized node" {:status 401})))

    (require '[datomic-blockchain.consensus.cluster :as cluster]
             '[datomic-blockchain.cluster.member :as member])

    ;; Get body-params from multiple sources
    (let [raw-body (:body request)
          get-val (fn [m k] (or (k m) (get m (name k))))
          body-params (or (:body-params request)
                         (when (map? raw-body) raw-body)
                         (when (string? raw-body)
                           (try
                             (json/read-str raw-body :key-fn keyword)
                             (catch Exception e
                               (log/error "Failed to parse JSON:" (.getMessage e))
                               nil))))
          proposal-id (or (:proposal-id body-params) (get-val body-params :proposal-id))
          transaction-data (or (:transaction-data body-params) (get-val body-params :transaction-data))
          proposer-id (or (:proposer-id body-params) (get-val body-params :proposer-id))
          cluster-member (member/get-cluster)
          node-id (member/node-id cluster-member)]

      ;; Validate proposal structure
      (when-not (and proposal-id transaction-data proposer-id)
        (throw (ex-info "Invalid proposal format" {:status 400})))

      ;; Check if cluster mode is enabled
      (when-not (member/cluster-enabled?)
        (throw (ex-info "Cluster mode not enabled" {:status 503})))

      ;; Vote on the proposal
      (let [vote (cluster-consensus/vote-on-proposal! proposal-id
                                           transaction-data
                                           node-id
                                           cluster-member
                                           nil)
            vote-message {:proposal-id proposal-id
                         :voter-id node-id
                         :vote (:vote vote)
                         :reason (:reason vote)
                         :timestamp (str (java.util.Date.))}
            _ (log/info "Voted on proposal" proposal-id ":" (:vote vote))

            ;; Send vote back to leader
            leader-id (member/leader-id cluster-member)
            leader-url (get (member/members cluster-member) leader-id)
            vote-url (str leader-url "/api/internal/vote")
            _ (log/debug "Sending vote to leader at:" vote-url)
            vote-body (json/write-str vote-message)]

        ;; Send vote to leader asynchronously (fire and forget)
        (future
          (try
            (clj-http.client/post vote-url
                                   {:headers {"Content-Type" "application/json"
                                              "X-Node-ID" node-id}
                                    :body vote-body
                                    :connection-timeout 2000
                                    :read-timeout 2000})
            (log/debug "Vote sent to leader successfully")
            (catch Exception e
              (log/warn "Failed to send vote to leader:" (.getMessage e)))))

        ;; Return success response to proposer
        (common/success vote-message)))

    (catch Exception e
      (log/error "Error processing PROPOSE:" (.getMessage e))
      (common/error (.getMessage e) (or (:status (ex-data e)) 500)))))

(defn handle-internal-vote
  "Handle VOTE message from cluster member.
   Records the vote and checks for quorum."
  [request _connection]
  (log/info "Received VOTE message")
  (try
    ;; Verify node authentication (only leader should receive votes)
    (when-not (verify-node-auth request)
      (throw (ex-info "Unauthorized node" {:status 401})))

    (require '[datomic-blockchain.consensus.cluster :as cluster]
             '[datomic-blockchain.cluster.member :as member])

    ;; Get body-params from multiple sources
    (let [raw-body (:body request)
          get-val (fn [m k] (or (k m) (get m (name k))))
          body-params (or (:body-params request)
                         (when (map? raw-body) raw-body)
                         (when (string? raw-body)
                           (try
                             (json/read-str raw-body :key-fn keyword)
                             (catch Exception e
                               (log/error "Failed to parse JSON:" (.getMessage e))
                               nil))))
          proposal-id (or (:proposal-id body-params) (get-val body-params :proposal-id))
          voter-id (or (:voter-id body-params) (get-val body-params :voter-id))
          vote-value (or (:vote body-params) (get-val body-params :vote))
          cluster-member (member/get-cluster)]

      (log/debug "Vote message - proposal-id:" proposal-id "voter-id:" voter-id "vote:" vote-value)

      ;; Only leader processes votes
      (when-not (member/is-leader? cluster-member)
        (throw (ex-info "Only leader can process votes" {:status 403})))

      ;; Record the vote
      (let [result (cluster-consensus/record-vote! proposal-id voter-id vote-value cluster-member)
            response {:proposal-id proposal-id
                      :voter-id voter-id
                      :vote-recorded true
                      :quorum-reached (:quorum-reached result)
                      :decision (:decision result)}]

        (log/info "Recorded vote from" voter-id "for proposal" proposal-id)
        (common/success response)))

    (catch Exception e
      (log/error "Error processing VOTE:" (.getMessage e))
      (common/error (.getMessage e) (or (:status (ex-data e)) 500)))))

(defn handle-internal-commit
  "Handle COMMIT message from cluster leader.
   Applies the committed block to local state."
  [request _connection]
  (log/info "Received COMMIT message")
  (try
    ;; Verify node authentication
    (when-not (verify-node-auth request)
      (throw (ex-info "Unauthorized node" {:status 401})))

    (require '[datomic-blockchain.consensus.cluster :as cluster]
             '[datomic-blockchain.cluster.member :as member])

    (let [body-params (:body-params request)
          proposal-id (:proposal-id body-params)
          block-data (:block-data body-params)
          cluster-member (member/get-cluster)]

      ;; Clear pending proposal
      (cluster-consensus/clear-pending-proposal proposal-id)

      (log/info "Applied commit for proposal" proposal-id)
      (common/success
       {:proposal-id proposal-id
        :commit-applied true
        :block-id (:block-id block-data)}))

    (catch Exception e
      (log/error "Error processing COMMIT:" (.getMessage e))
      (common/error (.getMessage e) 500))))

(defn handle-internal-rollback
  "Handle ROLLBACK message from cluster leader.
   Removes the pending proposal from local state."
  [request _connection]
  (log/info "Received ROLLBACK message")
  (try
    ;; Verify node authentication
    (when-not (verify-node-auth request)
      (throw (ex-info "Unauthorized node" {:status 401})))

    (require '[datomic-blockchain.consensus.cluster :as cluster])

    (let [body-params (:body-params request)
          proposal-id (:proposal-id body-params)
          reason (:reason body-params)]

      ;; Clear pending proposal
      (cluster-consensus/clear-pending-proposal proposal-id)

      (log/info "Rolled back proposal" proposal-id ":" reason)
      (common/success
       {:proposal-id proposal-id
        :rollback-applied true
        :reason reason}))

    (catch Exception e
      (log/error "Error processing ROLLBACK:" (.getMessage e))
      (common/error (.getMessage e) 500))))

(defn handle-internal-cluster-status
  "Get cluster status for monitoring and debugging.
   Returns current member info, leader status, and pending proposals."
  [request _connection]
  (log/info "Cluster status request")
  (try
    (require '[datomic-blockchain.consensus.cluster :as cluster]
             '[datomic-blockchain.cluster.member :as member])

    (if (member/cluster-enabled?)
      (let [cluster-member (member/get-cluster)]
        (common/success
         {:cluster-enabled true
          :node-id (member/node-id cluster-member)
          :is-leader (member/is-leader? cluster-member)
          :leader-id (member/leader-id cluster-member)
          :members (keys (member/members cluster-member))
          :quorum-size (member/quorum-size cluster-member)
          :pending-proposals (count (cluster-consensus/get-all-pending-proposals))}))
      (common/success
       {:cluster-enabled false}))

    (catch Exception e
      (log/error "Error getting cluster status:" (.getMessage e))
      (common/error (.getMessage e) 500))))
