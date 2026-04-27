(ns datomic-blockchain.permission.model
  "Protocol-based permission model for extensibility
  Clojure pattern: Protocols + Records for polymorphism"
  (:require [taoensso.timbre :as log]
            [datomic.api :as d]
            [datomic-blockchain.permission.checker :as checker])
  (:import [java.util UUID Date]))

;; ============================================================================
;; Permission Strategy Protocol
;; ============================================================================

(defprotocol PermissionStrategy
  "Protocol for permission checking strategies
  Allows different permission models to be plugged in"
  (check-access [this policy request] "Check access according to strategy")
  (can-grant? [this granter policy] "Check if granter can grant access")
  (can-revoke? [this revoker policy] "Check if revoker can revoke access"))

;; ============================================================================
;; Default Permission Strategy (Visibility-Based)
;; ============================================================================

(defrecord VisibilityStrategy []
  PermissionStrategy
  (check-access [this policy request]
    (log/debug "Using visibility-based strategy")
    (checker/check-permission policy request))

  (can-grant? [this granter policy]
    (log/debug "Checking if" granter "can grant access")
    (= (:owner-id policy) granter))

  (can-revoke? [this revoker policy]
    (log/debug "Checking if" revoker "can revoke access")
    (= (:owner-id policy) revoker)))

(defn visibility-strategy
  "Create a visibility-based permission strategy"
  []
  (->VisibilityStrategy))

;; ============================================================================
;; Role-Based Access Control (RBAC) Strategy
;; ============================================================================

(defrecord RBACStrategy [role-permissions]
  PermissionStrategy
  (check-access [this policy request]
    (log/debug "Using RBAC strategy")
    (let [requestor-id (:requestor-id request)
          requested-action (:requested-action request)
          ;; For simplicity, check if owner or has role permissions
          is-owner? (= (:owner-id policy) requestor-id)
          user-roles (get-in policy [:roles requestor-id] #{})
          permitted-actions (reduce (fn [acc role]
                                     (conj acc (get role-permissions role #{})))
                                   #{}
                                   user-roles)]
      (if (or is-owner? (contains? permitted-actions requested-action))
        {:success? true
         :reason :role-authorized
         :message "Access granted based on role"}
        {:success? false
         :reason :role-not-authorized
         :error-message "Role not authorized for this action"})))

  (can-grant? [this granter policy]
    ;; Admin roles can grant
    (let [granter-roles (get-in policy [:roles granter] #{})]
      (some #(contains? #{:admin :owner} %) granter-roles)))

  (can-revoke? [this revoker policy]
    ;; Admin roles can revoke
    (let [revoker-roles (get-in policy [:roles revoker] #{})]
      (some #(contains? #{:admin :owner} %) revoker-roles))))

(defn rbac-strategy
  "Create an RBAC strategy with role permissions"
  [role-permissions]
  {:pre [(map? role-permissions)]}
  (->RBACStrategy role-permissions))

;; ============================================================================
;; Attribute-Based Access Control (ABAC) Strategy
;; ============================================================================

(defn evaluate-rules
  "Evaluate ABAC rules
  For now, simple implementation - can be extended"
  [rules attrs action context]
  (every? (fn [[rule-key rule-value]]
            (case rule-key
              :action (= action rule-value)
              :department (= (:department attrs) rule-value)
              :level (>= (:level attrs) rule-value)
              :location (= (:location context) rule-value)
              true))
          rules))

(defrecord ABACStrategy [attribute-rules]
  PermissionStrategy
  (check-access [this policy request]
    (log/debug "Using ABAC strategy")
    (let [requestor-id (:requestor-id request)
          requested-action (:requested-action request)
          requestor-attrs (get-in policy [:attributes requestor-id] {})
          context (:request-context request {})]
      ;; Evaluate rules
      (if (evaluate-rules attribute-rules requestor-attrs requested-action context)
        {:success? true
         :reason :attribute-authorized
         :message "Access granted based on attributes"}
        {:success? false
         :reason :attribute-not-authorized
         :error-message "Attributes do not permit access"})))

  (can-grant? [this granter policy]
    ;; Can grant if has admin attribute
    (let [granter-attrs (get-in policy [:attributes granter] {})]
      (= :admin (:admin granter-attrs))))

  (can-revoke? [this revoker policy]
    ;; Can revoke if has admin attribute
    (let [revoker-attrs (get-in policy [:attributes revoker] {})]
      (= :admin (:admin revoker-attrs)))))

(defn abac-strategy
  "Create an ABAC strategy with attribute rules"
  [attribute-rules]
  {:pre [(map? attribute-rules)]}
  (map->ABACStrategy {:attribute-rules attribute-rules}))

;; ============================================================================
;; Permission Store Protocol
;; ============================================================================

(defprotocol PermissionStore
  "Protocol for permission storage backends"
  (get-policy [this resource-id] "Retrieve policy by resource ID")
  (save-policy! [this policy] "Save or update policy")
  (delete-policy! [this resource-id] "Delete policy by resource ID")
  (list-policies [this] "List all policies")
  (find-policies-by-owner [this owner-id] "Find all policies owned by user"))

;; ============================================================================
;; Datomic Permission Store Implementation
;; ============================================================================

;; Helper function - must be defined before use
(defn entity->map
  "Convert Datomic entity to map"
  [entity]
  (into {} (for [k (keys entity)]
            [k (get entity k)])))

(defrecord DatomicPermissionStore [conn]
  PermissionStore
  (get-policy [this resource-id]
    (log/debug "Fetching policy for resource:" resource-id)
    (let [db (d/db conn)
          policy-entity (d/entity db resource-id)]
      (when policy-entity
        (.touch policy-entity)
        (entity->map policy-entity))))

  (save-policy! [this policy]
    (log/info "Saving policy for resource:" (:resource-id policy))
    @(d/transact conn [policy])
    policy)

  (delete-policy! [this resource-id]
    (log/warn "Deleting policy for resource:" resource-id)
    ;; Retract all attributes of the policy entity
    @(d/transact conn [[:db.fn/retractAttribute resource-id]]))

  (list-policies [this]
    (log/debug "Listing all policies")
    (let [db (d/db conn)]
      (d/q '[:find [?e ?visibility]
             :where
             [?e :permission/visibility ?visibility]]
           db)))

  (find-policies-by-owner [this owner-id]
    (log/debug "Finding policies for owner:" owner-id)
    (let [db (d/db conn)]
      (d/q '[:find [?e ?visibility]
             :in $ ?owner
             :where
             [?e :permission/owner ?owner]
             [?e :permission/visibility ?visibility]]
           db
           owner-id))))

(defn datomic-permission-store
  "Create a Datomic-backed permission store"
  [conn]
  (->DatomicPermissionStore conn))

;; ============================================================================
;; Usage Examples
;; ============================================================================

(comment
  ;; Create strategies
  (def vis-strat (visibility-strategy))
  (def rbac-strat (rbac-strategy {:admin #{:read :write :delete :admin}
                                  :user #{:read}
                                  :editor #{:read :write}}))

  ;; Create store
  (require '[datomic-blockchain.datomic.connection :as conn])
  (def store (datomic-permission-store (dev/conn)))

  ;; Use strategy to check access
  (check-access vis-strat policy request)

  ;; Save policy
  (save-policy! store policy)

  ;; Get policy
  (get-policy store resource-id))
