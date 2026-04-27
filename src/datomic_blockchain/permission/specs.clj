(ns datomic-blockchain.permission.specs
  "Clojure specs for permission and access control system
  Spec-first development approach for data validation"
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import [java.util UUID]))

;; ============================================================================
;; Basic Types
;; ============================================================================

(s/def ::uuid uuid?)
(s/def ::inst inst?)
(s/def ::keyword keyword?)
(s/def ::string string?)
(s/def ::boolean boolean?)

;; ============================================================================
;; Permission Types
;; ============================================================================

(s/def ::visibility #{:public :restricted :private})
(s/def ::action #{:read :write :update :delete :admin})
(s/def ::resource-type #{:entity :activity :agent :ontology :policy})
(s/def ::agent-type #{:organization :person :system})

;; ============================================================================
;; Permission ID
;; ============================================================================

(s/def ::permission-id ::uuid)
(s/def ::owner-id ::uuid)
(s/def ::resource-id ::uuid)
(s/def ::actor-id ::uuid)

;; ============================================================================
;; Permission Policy
;; ============================================================================

(s/def ::valid-from ::inst)
(s/def ::valid-until ::inst)
(s/def ::max-depth (s/int-in 1 100))

(s/def ::time-constraint
  (s/keys :req-un [::valid-from]
          :opt-un [::valid-until]))

(s/def ::depth-constraint
  (s/keys :req-un [::max-depth]))

(s/def ::access-count-limit
  (s/int-in 1 1000000))

(s/def ::access-count-constraint
  (s/keys :req-un [::access-count-limit]))

;; ============================================================================
;; Permission Policy (Complete)
;; ============================================================================

(s/def ::permission-policy
  (s/keys :req-un [::permission-id
                   ::owner-id
                   ::resource-id
                   ::resource-type
                   ::visibility]
          :opt-un [::valid-from
                   ::valid-until
                   ::max-depth
                   ::authorized-parties]))

;; ============================================================================
;; Access Request
;; ============================================================================

(s/def ::request-time ::inst)
(s/def ::requestor-id ::uuid)
(s/def ::requested-action ::action)
(s/def ::request-context map?)

(s/def ::access-request
  (s/keys :req-un [::requestor-id
                   ::resource-id
                   ::requested-action
                   ::request-time]
          :opt-un [::request-context]))

;; ============================================================================
;; Access Result
;; ============================================================================

(s/def ::success? boolean?)
(s/def ::reason keyword?)
(s/def ::error-message string?)

(s/def ::access-result
  (s/keys :req-un [::success?]
          :opt-un [::reason
                   ::error-message
                   ::granted-permissions]))

;; ============================================================================
;; Agent (User/Organization)
;; ============================================================================

(s/def ::agent-name string?)
(s/def ::agent-email string?)
(s/def ::agent-roles (s/coll-of keyword? :min-count 0))

(s/def ::agent
  (s/keys :req-un [::actor-id
                   ::agent-type]
          :opt-un [::agent-name
                   ::agent-email
                   ::agent-roles]))

;; ============================================================================
;; Role-Based Access Control (RBAC)
;; ============================================================================

(s/def ::role keyword?)
(s/def ::permissions (s/coll-of ::action))
(s/def ::role-definition
  (s/keys :req-un [::role ::permissions]))

;; ============================================================================
;; Attribute-Based Access Control (ABAC)
;; ============================================================================

(s/def ::attribute-name keyword?)
(s/def ::attribute-value any?)
(s/def ::attribute (s/keys :req-un [::attribute-name ::attribute-value]))
(s/def ::attributes (s/coll-of ::attribute))

(s/def ::abac-rule
  (s/keys :req-un [::role ::resource-type ::attributes]
          :opt-un [::conditions]))

;; ============================================================================
;; Policy Conditions
;; ============================================================================

(s/def ::condition-type #{:time-based :location-based :count-based :context-based})
(s/def ::condition-operator #{:eq :ne :gt :lt :gte :lte :in :not-in})
(s/def ::condition-value any?)

(s/def ::condition
  (s/keys :req-un [::condition-type
                   ::condition-operator
                   ::condition-value]))

(s/def ::conditions (s/coll-of ::condition))

;; ============================================================================
;; Audit Log
;; ============================================================================

(s/def ::audit-id ::uuid)
(s/def ::timestamp ::inst)
(s/def ::access-granted boolean?)
(s/def ::checked-by ::uuid)

(s/def ::audit-entry
  (s/keys :req-un [::audit-id
                   ::actor-id
                   ::resource-id
                   ::requested-action
                   ::access-granted
                   ::timestamp]
          :opt-un [::checked-by
                   ::reason]))

;; ============================================================================
;; Helper Functions
;; ============================================================================

(defn valid-permission-policy?
  "Check if permission policy is valid according to spec"
  [policy]
  (s/valid? ::permission-policy policy))

(defn valid-access-request?
  "Check if access request is valid"
  [request]
  (s/valid? ::access-request request))

(defn explain-permission-policy
  "Explain why permission policy is invalid (if it is)"
  [policy]
  (s/explain-str ::permission-policy policy))

(defn explain-access-request
  "Explain why access request is invalid"
  [request]
  (s/explain-str ::access-request request))

;; ============================================================================
;; Generators for Testing
;; ============================================================================

(defn generate-permission-id
  "Generate a random permission ID"
  []
  (UUID/randomUUID))

(defn generate-permission-policy
  "Generate a valid permission policy for testing"
  ([] (generate-permission-policy {}))
  ([overrides]
   (merge {:permission-id (UUID/randomUUID)
            :owner-id (UUID/randomUUID)
            :resource-id (UUID/randomUUID)
            :resource-type :entity
            :visibility :private}
          overrides)))

(defn generate-access-request
  "Generate a valid access request for testing"
  ([] (generate-access-request {}))
  ([overrides]
   (merge {:requestor-id (UUID/randomUUID)
            :resource-id (UUID/randomUUID)
            :requested-action :read
            :request-time (java.util.Date.)}
          overrides)))

;; ============================================================================
;; Spec Assertions for Development
;; ============================================================================

(comment
  ;; Enable spec assertions during development
  (s/check-asserts true)

  ;; Test a permission policy
  (def test-policy (generate-permission-policy))
  (valid-permission-policy? test-policy)  ; => true

  ;; Test with spec explain
  (explain-permission-policy test-policy)  ; => nil (valid)

  ;; Invalid policy
  (def invalid-policy {:permission-id (UUID/randomUUID)})
  (valid-permission-policy? invalid-policy)  ; => false
  (explain-permission-policy invalid-policy)  ; => explanation

  ;; Generate test data
  (gen/generate (s/gen ::permission-policy))
  (gen/generate (s/gen ::access-request)))
