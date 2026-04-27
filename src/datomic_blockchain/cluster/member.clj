(ns datomic-blockchain.cluster.member
  "Cluster membership management for multi-node Datomic blockchain.

   Implements:
   - Node membership tracking
   - Heartbeat-based health monitoring
   - Leader election via first-available heuristic
   - HTTP-based inter-node communication"
  (:require [clojure.string :as str]
            [taoensso.timbre :as log]
            [clj-http.client :as http])
  (:import [java.time Instant]
           [java.util UUID]))

;; Cluster member state
(defonce cluster-state (atom nil))

(defprotocol ClusterMember
  "Protocol for cluster membership operations."
  (node-id [this] "Returns this node's ID")
  (is-leader? [this] "Returns true if this node is the leader")
  (leader-id [this] "Returns the current leader's node ID")
  (members [this] "Returns all cluster members")
  (quorum-size [this] "Returns the number of votes needed for quorum")
  (send-message [this node-id endpoint payload] "Send message to cluster member"))

(defrecord Member [node-id port members-list quorum leader-atom heartbeat-interval-ms]
  ClusterMember
  (node-id [this] (:node-id this))

  (is-leader? [this] (= @leader-atom (:node-id this)))

  (leader-id [this] @leader-atom)

  (members [this] (:members-list this))

  (quorum-size [this] (:quorum this))

  (send-message [this target-node-id endpoint payload]
    (let [member-url (get (:members-list this) target-node-id)]
      (when member-url
        (try
          (let [response (http/post (str member-url endpoint)
                                    {:body payload
                                     :headers {"Content-Type" "application/json"
                                               "X-Node-ID" (:node-id this)}
                                     :connect-timeout 2000
                                     :read-timeout 2000
                                     :throw-exceptions false})]
            (when (= (:status response) 200)
              (:body response)))
          (catch Exception e
            (log/warn "Failed to send message to" target-node-id ":" (.getMessage e))
            nil))))))

;; Leader election: first available node becomes leader
(defn elect-leader!
  "Elects a leader from the cluster members.
   Simple heuristic: first node in sorted member list becomes leader.
   This works for stable 2-3 node clusters with deterministic ordering."
  [member]
  (let [all-members (keys (:members-list member))
        sorted-members (sort all-members)
        new-leader (first sorted-members)]
    (reset! (:leader-atom member) new-leader)
    (log/info "Leader elected:" new-leader "from cluster" sorted-members)
    new-leader))

(defn parse-cluster-members
  "Parse CLUSTER_MEMBERS env var into map of node-id -> base-url.
   Supports formats:
   - 'node-1:3001,node-2:3002' (simple port)
   - 'node-1:172.28.0.11:3001,node-2:172.28.0.12:3002' (host:port)"
  [env-value]
  (when env-value
    (->> (str/split env-value #",")
         (map str/trim)
         (filter seq)
         (map #(str/split % #":"))
         (filter #(>= (count %) 2))
         (map #(if (= 3 (count %))
                 ;; Format: node-id:host:port
                 [(first %) (str "http://" (second %) ":" (nth % 2))]
                 ;; Format: node-id:port (localhost assumed)
                 [(first %) (str "http://localhost:" (second %))]))
         (into {}))))

(defn create-member
  "Create a new cluster member instance.

   Configuration via environment variables:
   - NODE_ID: This node's identifier (default: random UUID)
   - CLUSTER_MEMBERS: Comma-separated list of 'node-id:port' pairs
   - QUORUM_SIZE: Number of votes needed for consensus (default: 2)"
  []
  (let [node-id (or (System/getenv "NODE_ID")
                    (str "node-" (UUID/randomUUID)))
        cluster-members (parse-cluster-members (System/getenv "CLUSTER_MEMBERS"))
        quorum (or (some-> (System/getenv "QUORUM_SIZE") Integer/parseInt)
                   (max 2 (int (Math/ceil (* (count cluster-members) 0.5)))))
        this-node-port (or (some-> (System/getenv "SERVER_PORT") Integer/parseInt) 3000)

        ;; Build members map including this node if not listed
        all-members (if (contains? cluster-members node-id)
                      cluster-members
                      (assoc cluster-members node-id (str "http://localhost:" this-node-port)))

        leader-atom (atom nil)]

    (log/info "Creating cluster member:" node-id)
    (log/info "Cluster members:" (keys all-members))
    (log/info "Quorum size:" quorum)

    (->Member node-id
              this-node-port
              all-members
              quorum
              leader-atom
              5000 ;; heartbeat-interval-ms
              )))

(defn start-heartbeat!
  "Start heartbeat monitoring to detect failed nodes.

   For a 2-3 node cluster, this provides:
   - Failure detection within 2-3 heartbeat intervals
   - Automatic leader re-election if leader fails
   - Health status for all cluster members"
  [member]
  (future
    (log/info "Starting heartbeat monitor for" (node-id member))
    (while true
      (try
        ;; Check health of all members
        (doseq [[member-id member-url] (members member)]
          (when (not= member-id (node-id member))
            (try
              (let [response (http/get (str member-url "/health")
                                       {:connect-timeout 1000
                                        :read-timeout 1000
                                        :throw-exceptions false})]
                (when (not= (:status response) 200)
                  (log/warn "Node" member-id "health check failed:" (:status response)))
                ;; If leader fails, re-elect
                (when (and (= member-id @(:leader-atom member))
                           (not= (:status response) 200))
                  (log/warn "Leader" member-id "failed, re-electing...")
                  (elect-leader! member)))
              (catch Exception e
                (log/warn "Heartbeat to" member-id "failed:" (.getMessage e))
                ;; If leader fails, re-elect
                (when (= member-id @(:leader-atom member))
                  (log/warn "Leader" member-id "unreachable, re-electing...")
                  (elect-leader! member)))))

        ;; Sleep for heartbeat interval
        (Thread/sleep (:heartbeat-interval-ms member)))
      (catch Exception e
        (log/error "Heartbeat error:" (.getMessage e))
        (Thread/sleep 1000))))))

(defn initialize-cluster!
  "Initialize the cluster for this node.

   Returns the Member instance after:
   1. Creating member record
   2. Running leader election
   3. Starting heartbeat monitor"
  []
  (let [member (create-member)]
    ;; Initial leader election
    (elect-leader! member)
    ;; Start heartbeat monitoring
    (start-heartbeat! member)
    ;; Store in global atom
    (reset! cluster-state member)
    (log/info "Cluster initialized. Leader:" (leader-id member))
    member))

(defn get-cluster
  "Get the current cluster member instance."
  []
  @cluster-state)

(defn cluster-enabled?
  "Check if cluster mode is enabled via environment variable."
  []
  (Boolean/parseBoolean (System/getenv "CLUSTER_MODE")))
