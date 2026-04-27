(ns datomic-blockchain.permission.policy
  "Policy management with atoms
  Clojure pattern: Atoms for coordinated state management"
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [datomic-blockchain.permission.specs :as specs]
            [datomic-blockchain.permission.checker :as checker]
            [datomic-blockchain.permission.model :as model]
            [datomic-blockchain.permission.proof :as proof])
  (:import [java.util UUID]))

;; ============================================================================
;; Policy Store (Atom-based)
;; ============================================================================

(def ^:private policy-store-atom (atom nil))

(defn- normalize-strategy
  [strategy]
  (cond
    (nil? strategy) (model/visibility-strategy)
    (= strategy :default) (model/visibility-strategy)
    :else strategy))

(defn init-policy-store
  "Initialize the policy store with a strategy"
  ([strategy]
   (init-policy-store strategy {}))
  ([strategy initial-policies]
   (let [strategy (normalize-strategy strategy)]
     (log/info "Initializing policy store with strategy:" (class strategy))
   (reset! policy-store-atom
           {:strategy strategy
            :policies (atom initial-policies)
            :audit-log (atom [])}))))

(defn policy-store
  "Get the policy store"
  []
  @policy-store-atom)

(defn strategy
  "Get current strategy"
  []
  (:strategy (policy-store)))

;; ============================================================================
;; Policy Management (Coordinated with Atoms)
;; ============================================================================

(defn add-policy!
  "Add a policy to the store (coordinated with swap!)
  Returns the added policy"
  [policy]
  {:post [(s/valid? ::specs/permission-policy %)]}
  (log/info "Adding policy for resource:" (:resource-id policy))
  (swap! (:policies (policy-store))
         assoc
         (:resource-id policy)
         policy)
  policy)

(defn get-policy
  "Get policy by resource ID"
  [resource-id]
  (log/debug "Getting policy for resource:" resource-id)
  (get @(:policies (policy-store)) resource-id))

(defn update-policy!
  "Update a policy (coordinated with swap!)
  Returns updated policy"
  [resource-id update-fn]
  (log/info "Updating policy for resource:" resource-id)
  (swap! (:policies (policy-store))
         update
         resource-id
         update-fn)
  (get-policy resource-id))

(defn remove-policy!
  "Remove a policy from the store"
  [resource-id]
  (log/info "Removing policy for resource:" resource-id)
  (swap! (:policies (policy-store))
         dissoc
         resource-id)
  nil)

(defn list-policies
  "List all policies"
  []
  (log/debug "Listing all policies")
  (vals @(:policies (policy-store))))

(defn find-policies-by-owner
  "Find all policies owned by a specific owner"
  [owner-id]
  (log/debug "Finding policies for owner:" owner-id)
  (filter #(= (:owner-id %) owner-id)
          (vals @(:policies (policy-store)))))

;; ============================================================================
;; Permission Checking with Strategy
;; ============================================================================

(defn check-access
  "Check access using the configured strategy"
  [resource-id request]
  (log/debug "Checking access for resource:" resource-id)
  (if-let [policy (get-policy resource-id)]
    (let [result (model/check-access (strategy) policy request)]
      (merge result
             {:resource-id resource-id
              :requestor-id (:requestor-id request)
              :requested-action (:requested-action request)}))
    {:success? false
     :reason :no-policy
     :error-message "No policy found for resource"
     :resource-id resource-id
     :requestor-id (:requestor-id request)
     :requested-action (:requested-action request)}))

(defn can-access?
  "Quick boolean check for access"
  [resource-id request]
  (:success? (check-access resource-id request)))

;; ============================================================================
;; Grant/Revoke Access with Atoms
;; ============================================================================

(defn grant-access!
  "Grant access to a party using coordinated state update
  Returns updated policy"
  [resource-id party-id]
  (log/info "Granting access to" party-id "for resource:" resource-id)
  (update-policy! resource-id
                  #(checker/grant-access % party-id)))

(defn revoke-access!
  "Revoke access from a party using coordinated state update
  Returns updated policy"
  [resource-id party-id]
  (log/info "Revoking access from" party-id "for resource:" resource-id)
  (update-policy! resource-id
                  #(checker/revoke-access % party-id)))

(defn set-visibility!
  "Set visibility using coordinated state update"
  [resource-id visibility]
  (log/info "Setting visibility to" visibility "for resource:" resource-id)
  (update-policy! resource-id
                  #(checker/set-visibility % visibility)))

(defn set-time-constraints!
  "Set time constraints using coordinated state update"
  [resource-id valid-from valid-until]
  (log/info "Setting time constraints for resource:" resource-id)
  (update-policy! resource-id
                  #(checker/set-time-constraints % valid-from valid-until)))

;; ============================================================================
;; Batch Operations
;; ============================================================================

(defn grant-access-batch!
  "Grant access to multiple resources for a party
  Returns vector of updated policies"
  [party-id resource-ids]
  (log/info "Batch granting access to" (count resource-ids) "resources for" party-id)
  (mapv (fn [resource-id]
          (grant-access! resource-id party-id))
        resource-ids))

(defn revoke-access-batch!
  "Revoke access from multiple resources for a party
  Returns vector of updated policies"
  [party-id resource-ids]
  (log/info "Batch revoking access from" (count resource-ids) "resources for" party-id)
  (mapv (fn [resource-id]
          (revoke-access! resource-id party-id))
        resource-ids))

;; ============================================================================
;; Audit Logging
;; ============================================================================

(defn log-access
  "Log an access attempt to the audit log (coordinated)"
  [access-result]
  (log/debug "Logging access result:" (:success? access-result))
  (swap! (:audit-log (policy-store))
         conj
         (merge access-result
                {:timestamp (java.util.Date.)
                 :audit-id (UUID/randomUUID)
                 :access-granted (:success? access-result)})))

(defn check-access-with-proof
  "Check access and generate a proof for the decision.

  Options:
  - :audit? true/false to control audit logging (default true)
  - :signing proof signing options (see permission.proof/build-proof)"
  ([resource-id request]
   (check-access-with-proof resource-id request {}))
  ([resource-id request opts]
   (let [policy (get-policy resource-id)
         result (check-access resource-id request)
         proof (proof/build-proof policy request result opts)
         result-with-proof (assoc result :proof proof)]
     (when (get opts :audit? true)
       (log-access result-with-proof))
     result-with-proof)))

(defn get-audit-log
  "Get the entire audit log"
  []
  @(:audit-log (policy-store)))

(defn get-audit-log-for-resource
  "Get audit log entries for a specific resource"
  [resource-id]
  (filter #(= (:resource-id %) resource-id)
          (get-audit-log)))

(defn get-audit-log-for-actor
  "Get audit log entries for a specific actor"
  [actor-id]
  (filter #(= (:requestor-id %) actor-id)
          (get-audit-log)))

(defn clear-audit-log!
  "Clear the audit log (use with caution)"
  []
  (log/warn "Clearing audit log")
  (reset! (:audit-log (policy-store)) []))

;; ============================================================================
;; Statistics and Monitoring
;; ============================================================================

(defn get-statistics
  "Get statistics about the policy store"
  []
  (let [policies (list-policies)
        audit (get-audit-log)]
    {:total-policies (count policies)
     :public-policies (count (filter #(= :public (:visibility %)) policies))
     :private-policies (count (filter #(= :private (:visibility %)) policies))
     :restricted-policies (count (filter #(= :restricted (:visibility %)) policies))
     :total-audit-entries (count audit)
     :granted-access (count (filter #(true? (:access-granted %)) audit))
     :denied-access (count (filter #(false? (:access-granted %)) audit))}))

;; ============================================================================
;; High-Level Convenience Functions
;; ============================================================================

(defn create-public-resource!
  "Create a new public resource policy"
  [owner-id resource-id]
  (log/info "Creating public resource:" resource-id)
  (add-policy! (checker/public-policy owner-id resource-id)))

(defn create-private-resource!
  "Create a new private resource policy"
  [owner-id resource-id]
  (log/info "Creating private resource:" resource-id)
  (add-policy! (checker/private-policy owner-id resource-id)))

(defn create-restricted-resource!
  "Create a new restricted resource policy"
  [owner-id resource-id authorized-parties]
  (log/info "Creating restricted resource:" resource-id)
  (add-policy! (checker/restricted-policy owner-id resource-id authorized-parties)))

;; ============================================================================
;; Datomic Integration
;; ============================================================================

(defn sync-to-datomic!
  "Synchronize all policies to Datomic
  Useful for persistence"
  [conn]
  (log/info "Syncing policies to Datomic...")
  (let [store (model/datomic-permission-store conn)]
    (doseq [policy (list-policies)]
      (model/save-policy! store policy)))
  (count (list-policies)))

(defn load-from-datomic!
  "Load policies from Datomic into the store"
  [conn]
  (log/info "Loading policies from Datomic...")
  (let [store (model/datomic-permission-store conn)
        policies (model/list-policies store)]
    (doseq [[resource-id _] policies]
      (when-let [policy (model/get-policy store resource-id)]
        (add-policy! policy)))
    (count policies)))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Initialize store
  (require '[datomic-blockchain.permission.model :as model])
  (init-policy-store (model/visibility-strategy))

  ;; Create policies
  (def owner (random-uuid))
  (def resource1 (random-uuid))
  (def resource2 (random-uuid))

  (create-public-resource! owner resource1)
  (create-private-resource! owner resource2)

  ;; Check access
  (def request (specs/generate-access-request
                 {:requestor-id owner
                  :resource-id resource1
                  :requested-action :read}))

  (can-access? resource1 request)
  (check-access resource1 request)

  ;; Grant/revoke
  (grant-access! resource2 (random-uuid))
  (revoke-access! resource2 (first (:authorized-parties (get-policy resource2))))

  ;; Statistics
  (get-statistics)

  ;; Audit log
  (log-access (check-access resource1 request))
  (get-audit-log)

  ;; Sync to Datomic
  (sync-to-datomic! (dev/conn)))
