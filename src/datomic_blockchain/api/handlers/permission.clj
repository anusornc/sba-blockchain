(ns datomic-blockchain.api.handlers.permission
  "Permission and access control handlers"
  (:require [taoensso.timbre :as log]
            [datomic-blockchain.api.handlers.common :as common]
            [datomic-blockchain.permission.policy :as policy]))

;; ============================================================================
;; Permission Handlers
;; ============================================================================

(defn handle-check-permission
  "Check access permission"
  [request connection policy-store]
  (common/with-error-handling "Check permission"
    (let [resource-id (get-in request [:params :resource-id])
          requestor-id (get-in request [:params :requestor-id])
          action (keyword (or (get-in request [:params :action]) "read"))]

      (log/info "Check permission:" resource-id "for" requestor-id)

      (if policy-store
        (let [result (policy/can-access? policy-store
                                        resource-id
                                        requestor-id
                                        action)]
          (common/success
           {:resource-id resource-id
            :requestor-id requestor-id
            :action action
            :allowed result}))
        (common/error "Policy store not initialized" 503)))))
