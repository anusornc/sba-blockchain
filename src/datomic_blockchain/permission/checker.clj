(ns datomic-blockchain.permission.checker
  "Pure functions for permission checking
  Follows Clojure patterns: pure, composable, explicit"
  (:require [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [datomic-blockchain.permission.specs :as specs])
  (:import [java.util Date]))

;; ============================================================================
;; Pure Permission Checking Functions
;; ============================================================================

(defn policy-matches-resource?
  "Check if policy applies to requested resource"
  [policy resource-id]
  (= (:resource-id policy) resource-id))

(defn within-time-range?
  "Pure function: Check if current time is within policy's valid time range"
  [policy current-time]
  (let [valid-from (:valid-from policy)
        valid-until (:valid-until policy)]
    (cond
      ;; No time constraints
      (and (nil? valid-from) (nil? valid-until))
      true

      ;; Only valid-from specified
      (and valid-from (nil? valid-until))
      (.after current-time valid-from)

      ;; Only valid-until specified
      (and (nil? valid-from) valid-until)
      (.before current-time valid-until)

      ;; Both specified
      :else
      (and (.after current-time valid-from)
           (.before current-time valid-until)))))

(defn owner-access?
  "Pure function: Check if requestor is the owner"
  [policy requestor-id]
  (= (:owner-id policy) requestor-id))

(defn authorized-party-access?
  "Pure function: Check if requestor is in authorized parties list"
  [policy requestor-id]
  (some #(= % requestor-id) (:authorized-parties policy)))

(defn action-allowed?
  "Pure function: Check if requested action is allowed
  Admin actions only for owner, read/write for authorized"
  [policy requestor-id requested-action]
  (let [is-owner? (owner-access? policy requestor-id)]
    (cond
      ;; Admin actions require ownership
      (= requested-action :admin)
      is-owner?

      ;; Delete actions require ownership
      (= requested-action :delete)
      is-owner?

      ;; Read/write allowed for owner or authorized
      (or (= requested-action :read)
          (= requested-action :write)
          (= requested-action :update))
      (or is-owner?
          (authorized-party-access? policy requestor-id))

      ;; Unknown action
      :else
      false)))

(defn can-access?
  "Main pure function: Check if requestor can access resource
  Returns boolean - suitable for guard clauses"
  [policy request]
  {:pre [(s/valid? ::specs/permission-policy policy)
         (s/valid? ::specs/access-request request)]}
  (let [requestor-id (:requestor-id request)
        resource-id (:resource-id request)
        requested-action (:requested-action request)
        request-time (:request-time request)
        visibility (:visibility policy)]

    (log/debug "Checking access:"
               "requestor=" requestor-id
               "resource=" resource-id
               "visibility=" visibility)

    ;; Policy must match resource
    (and (policy-matches-resource? policy resource-id)

         ;; Check visibility rules
         (case visibility

           ;; Public: always accessible
           :public
           (action-allowed? policy requestor-id requested-action)

           ;; Private: only owner
           :private
           (and (owner-access? policy requestor-id)
                (action-allowed? policy requestor-id requested-action))

           ;; Restricted: owner or authorized parties
           :restricted
           (and (or (owner-access? policy requestor-id)
                    (authorized-party-access? policy requestor-id))
                (action-allowed? policy requestor-id requested-action)

                ;; Check time constraints if applicable
                (within-time-range? policy request-time))

           ;; Unknown visibility: deny
           :else
           false))))

;; ============================================================================
;; Detailed Permission Checking (with reasons)
;; ============================================================================

(defn check-permission
  "Detailed permission check with reason
  Returns map: {:success? boolean :reason keyword}"
  [policy request]
  {:post [(map? %)
          (contains? % :success?)
          (contains? % :reason)]}
  (let [requestor-id (:requestor-id request)
        resource-id (:resource-id request)
        requested-action (:requested-action request)
        request-time (:request-time request)
        visibility (:visibility policy)]

    (log/debug "Checking permission with details:"
               "visibility=" visibility
               "requestor=" requestor-id
               "resource=" resource-id
               "action=" requested-action)

    (cond
      ;; Resource mismatch
      (not (policy-matches-resource? policy resource-id))
      {:success? false
       :reason :resource-mismatch
       :error-message "Policy does not apply to this resource"}

      ;; Check visibility
      (= visibility :public)
      (if (action-allowed? policy requestor-id requested-action)
        {:success? true
         :reason :public
         :message "Public resource accessible"}
        {:success? false
         :reason :action-not-allowed
         :error-message "Action not allowed for this user"})

      (= visibility :private)
      (if (owner-access? policy requestor-id)
        (if (action-allowed? policy requestor-id requested-action)
          {:success? true
           :reason :owner
           :message "Owner access granted"}
          {:success? false
           :reason :action-not-allowed
           :error-message "Action not allowed"})
        {:success? false
         :reason :not-authorized
         :error-message "Private resource - owner only"})

      (= visibility :restricted)
      (cond
        ;; Check if within time range
        (and (:valid-from policy)
             (not (within-time-range? policy request-time)))
        {:success? false
         :reason :not-yet-valid
         :error-message "Access not yet valid"}

        (and (:valid-until policy)
             (not (within-time-range? policy request-time)))
        {:success? false
         :reason :expired
         :error-message "Access expired"}

        ;; Check authorization
        (or (owner-access? policy requestor-id)
             (authorized-party-access? policy requestor-id))
        (if (action-allowed? policy requestor-id requested-action)
          {:success? true
           :reason :authorized
           :message "Authorized access granted"}
          {:success? false
           :reason :action-not-allowed
           :error-message "Action not allowed"})

        :else
        {:success? false
         :reason :not-authorized
         :error-message "Not authorized for restricted resource"})

      :else
      {:success? false
       :reason :invalid-visibility
       :error-message "Invalid visibility level"})))

;; ============================================================================
;; Batch Permission Checks (Efficient)
;; ============================================================================

(defn check-permissions-batch
  "Check multiple permissions efficiently
  Returns vector of result maps in same order"
  [policies requests]
  {:pre [(= (count policies) (count requests))]}
  (log/debug "Batch checking" (count policies) "permissions")
  (mapv check-permission policies requests))

(defn filter-accessible
  "Filter list of resources to only those accessible by requestor
  Returns vector of accessible resources"
  [policies request]
  (log/debug "Filtering accessible resources for requestor:" (:requestor-id request))
  (filterv #(can-access? % request) policies))

;; ============================================================================
;; Policy Modification (Pure Functions)
;; ============================================================================

(defn grant-access
  "Pure function: Grant access to a party by adding to authorized list
  Returns new policy (immutable)"
  [policy party-id]
  {:post [(s/valid? ::specs/permission-policy %)]}
  (log/info "Granting access to" party-id "for resource:" (:resource-id policy))
  (let [current-authorized (:authorized-parties policy [])
        updated-authorized (conj (vec current-authorized) party-id)
        ;; Remove duplicates
        unique-authorized (vec (distinct updated-authorized))]
    (assoc policy :authorized-parties unique-authorized)))

(defn revoke-access
  "Pure function: Revoke access from a party
  Returns new policy (immutable)"
  [policy party-id]
  {:post [(s/valid? ::specs/permission-policy %)]}
  (log/info "Revoking access from" party-id "for resource:" (:resource-id policy))
  (let [current-authorized (:authorized-parties policy)
        updated-authorized (vec (remove #(= % party-id) current-authorized))]
    (assoc policy :authorized-parties updated-authorized)))

(defn set-visibility
  "Pure function: Set visibility level of policy
  Returns new policy (immutable)"
  [policy visibility]
  {:post [(s/valid? ::specs/permission-policy %)]}
  (log/info "Setting visibility to" visibility "for resource:" (:resource-id policy))
  (assoc policy :visibility visibility))

(defn set-time-constraints
  "Pure function: Set time constraints on policy
  Returns new policy (immutable)"
  [policy valid-from valid-until]
  {:post [(s/valid? ::specs/permission-policy %)]}
  (log/info "Setting time constraints for resource:" (:resource-id policy))
  (-> policy
      (assoc :valid-from valid-from)
      (assoc :valid-until valid-until)))

;; ============================================================================
;; Utility Functions
;; ============================================================================

(defn public-policy
  "Create a public access policy"
  [owner-id resource-id]
  (specs/generate-permission-policy
   {:owner-id owner-id
    :resource-id resource-id
    :visibility :public}))

(defn private-policy
  "Create a private access policy (owner only)"
  [owner-id resource-id]
  (specs/generate-permission-policy
   {:owner-id owner-id
    :resource-id resource-id
    :visibility :private}))

(defn restricted-policy
  "Create a restricted access policy"
  [owner-id resource-id authorized-parties]
  (specs/generate-permission-policy
   {:owner-id owner-id
    :resource-id resource-id
    :visibility :restricted
    :authorized-parties authorized-parties}))

;; ============================================================================
;; Development Helpers
;; ============================================================================

(comment
  ;; Usage examples

  ;; Create policies
  (def pub-policy (public-policy (random-uuid) (random-uuid)))
  (def priv-policy (private-policy (random-uuid) (random-uuid)))
  (def rest-policy (restricted-policy (random-uuid) (random-uuid) [(random-uuid)]))

  ;; Create request
  (def request (specs/generate-access-request
                {:requestor-id (random-uuid)
                 :requested-action :read}))

  ;; Check access (boolean)
  (can-access? pub-policy request)

  ;; Check with details
  (check-permission pub-policy request)

  ;; Grant access
  (def updated (grant-access rest-policy (random-uuid)))

  ;; Revoke access
  (def updated2 (revoke-access updated (first (:authorized-parties updated))))

  ;; Batch check
  (check-permissions-batch [pub-policy priv-policy]
                           [request request]))
