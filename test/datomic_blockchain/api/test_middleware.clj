(ns datomic-blockchain.api.test-middleware
  "Comprehensive test suite for API middleware.

   Tests cover:
   - JWT token generation and verification
   - Authentication middleware (required and optional)
   - Role-based authorization
   - JSON response generation
   - Error handling and sanitization
   - CORS middleware
   - Security headers
   - Request ID generation
   - Content negotiation
   - Response helpers (success, error, paginated)"
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [datomic-blockchain.api.middleware :as mw])
  (:import [java.util UUID Date]))

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn parse-json-body
  "Parse JSON response body to Clojure map"
  [response]
  (when-let [body (:body response)]
    (json/read-str body :key-fn keyword)))

(defn generate-valid-token
  "Generate a valid JWT token for testing"
  ([user-id]
   (generate-valid-token user-id nil))
  ([user-id opts]
   (mw/generate-token user-id opts)))

;; =============================================================================
;; JWT Token Generation Tests
;; =============================================================================

(deftest generate-token-default-test
  (testing "Generate token with default expiration"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id)]
      (is (string? token))
      (is (> (count token) 0))
      (is (= 3 (count (str/split token #"\.")))))))

(deftest generate-token-with-expiration-test
  (testing "Generate token with custom expiration"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id {:exp 7200})
          claims (mw/verify-token token)]
      (is (string? token))
      (is (some? claims))
      (is (= user-id (:sub claims)))
      (is (some? (:exp claims)))
      (is (integer? (:exp claims))))))

(deftest generate-token-with-roles-test
  (testing "Generate token with roles"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id {:roles [:admin :user]})
          claims (mw/verify-token token)]
      (is (string? token))
      (is (some? claims))
      (is (= user-id (:sub claims)))
      (is (vector? (:roles claims)))
      (is (= 2 (count (:roles claims)))))))

(deftest generate-token-has-iat-test
  (testing "Generated token includes issued-at time"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id)
          claims (mw/verify-token token)]
      (is (some? (:iat claims)))
      (is (integer? (:iat claims)))
      (is (> (:iat claims) 0)))))

;; =============================================================================
;; JWT Token Verification Tests
;; =============================================================================

(deftest verify-token-valid-test
  (testing "Verify valid token returns claims"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id)
          claims (mw/verify-token token)]
      (is (some? claims))
      (is (= user-id (:sub claims))))))

(deftest verify-token-invalid-test
  (testing "Verify invalid token returns nil"
    (is (nil? (mw/verify-token "invalid.token.here")))
    (is (nil? (mw/verify-token "")))
    (is (nil? (mw/verify-token nil)))))

;; =============================================================================
;; Auth Token Extraction Tests
;; =============================================================================

(deftest extract-auth-token-valid-test
  (testing "Extract Bearer token from headers"
    (let [token (generate-valid-token (str (UUID/randomUUID)))
          headers {"authorization" (str "Bearer " token)}]
      (is (= token (mw/extract-auth-token headers))))))

(deftest extract-auth-token-missing-header-test
  (testing "Return nil when authorization header missing"
    (is (nil? (mw/extract-auth-token {})))
    (is (nil? (mw/extract-auth-token {"authorization" ""})))
    (is (nil? (mw/extract-auth-token {"authorization" "invalid"})))))

;; =============================================================================
;; Valid Auth Token Tests
;; =============================================================================

(deftest valid-auth-token-valid-test
  (testing "Return claims for valid token"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id)
          claims (mw/valid-auth-token? token)]
      (is (some? claims))
      (is (= user-id (:sub claims))))))

(deftest valid-auth-token-invalid-test
  (testing "Return nil for invalid token"
    (is (nil? (mw/valid-auth-token? "invalid")))
    (is (nil? (mw/valid-auth-token? nil)))))

;; =============================================================================
;; Authentication Middleware Tests
;; =============================================================================

(deftest wrap-authenticated-valid-token-test
  (testing "Allow request with valid token"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id)
          handler (fn [req] {:status 200 :body (:user-claims req)})
          middleware (mw/wrap-authenticated handler)
          request {:headers {"authorization" (str "Bearer " token)}
                   :uri "/api/test"}
          response (middleware request)]
      (is (= 200 (:status response)))
      (is (some? (:body response)))
      (is (= user-id (:sub (:body response)))))))

(deftest wrap-authenticated-missing-token-test
  (testing "Reject request without token"
    (let [handler (fn [req] {:status 200})
          middleware (mw/wrap-authenticated handler)
          request {:headers {} :uri "/api/test"}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 401 (:status response)))
      (is (= "unauthorized" (:status body)))
      (is (= "Missing authorization header" (:error body))))))

(deftest wrap-authenticated-invalid-token-test
  (testing "Reject request with invalid token"
    (let [handler (fn [req] {:status 200})
          middleware (mw/wrap-authenticated handler)
          request {:headers {"authorization" "Bearer invalid"}
                   :uri "/api/test"}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 401 (:status response)))
      (is (= "unauthorized" (:status body)))
      (is (= "Invalid token" (:error body))))))

;; =============================================================================
;; Optional Authentication Tests
;; =============================================================================

(deftest wrap-optional-auth-with-token-test
  (testing "Add claims when valid token provided"
    (let [user-id (str (UUID/randomUUID))
          token (generate-valid-token user-id {:roles [:user]})
          handler (fn [req] {:status 200 :body (:user-claims req)})
          middleware (mw/wrap-optional-auth handler)
          request {:headers {"authorization" (str "Bearer " token)}}
          response (middleware request)]
      (is (some? (:body response)))
      (is (= user-id (:sub (:body response)))))))

(deftest wrap-optional-auth-without-token-test
  (testing "Allow request without token"
    (let [handler (fn [req] {:status 200 :body (:user-claims req)})
          middleware (mw/wrap-optional-auth handler)
          request {:headers {}}
          response (middleware request)]
      (is (= 200 (:status response)))
      (is (nil? (:body response))))))

(deftest wrap-optional-auth-invalid-token-test
  (testing "Allow request with invalid token (no claims added)"
    (let [handler (fn [req] {:status 200 :body (:user-claims req)})
          middleware (mw/wrap-optional-auth handler)
          request {:headers {"authorization" "Bearer invalid"}}
          response (middleware request)]
      (is (= 200 (:status response)))
      (is (nil? (:body response))))))

;; =============================================================================
;; Role-based Authorization Tests
;; =============================================================================

(deftest wrap-require-role-has-role-test
  (testing "Allow user with required role"
    (let [user-id (str (UUID/randomUUID))
          ;; JWT serialization converts keywords to strings
          token (generate-valid-token user-id {:roles ["admin" "user"]})
          handler (fn [req] {:status 200 :body "success"})
          middleware (mw/wrap-authenticated (mw/wrap-require-role handler "admin"))
          request {:headers {"authorization" (str "Bearer " token)}
                   :uri "/api/test"}
          response (middleware request)]
      (is (= 200 (:status response)))
      (is (= "success" (:body response))))))

(deftest wrap-require-role-missing-role-test
  (testing "Reject user without required role"
    (let [user-id (str (UUID/randomUUID))
          ;; JWT serialization converts keywords to strings
          token (generate-valid-token user-id {:roles ["user"]})
          handler (fn [req] {:status 200})
          middleware (mw/wrap-authenticated (mw/wrap-require-role handler "admin"))
          request {:headers {"authorization" (str "Bearer " token)}
                   :uri "/api/test"}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 403 (:status response)))
      (is (= "forbidden" (:status body)))
      (is (= "Insufficient permissions" (:error body))))))

(deftest wrap-require-role-unauthenticated-test
  (testing "Reject unauthenticated request - wrap-authenticated catches first"
    (let [handler (fn [req] {:status 200})
          middleware (mw/wrap-authenticated (mw/wrap-require-role handler "admin"))
          request {:headers {}}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 401 (:status response)))
      (is (= "unauthorized" (:status body)))
      ;; wrap-authenticated returns "Missing authorization header" when no token
      (is (= "Missing authorization header" (:error body))))))

;; =============================================================================
;; JSON Response Tests
;; =============================================================================

(deftest json-response-default-test
  (testing "Create JSON response with default 200 status"
    (let [data {:message "test"}
          response (mw/json-response data)
          body (parse-json-body response)]
      (is (= 200 (:status response)))
      (is (= "application/json" (get-in response [:headers "Content-Type"])))
      (is (= data body)))))

(deftest json-response-custom-status-test
  (testing "Create JSON response with custom status"
    (let [data {:error "not found"}
          response (mw/json-response data 404)
          body (parse-json-body response)]
      (is (= 404 (:status response)))
      (is (= data body)))))

(deftest json-response-with-headers-test
  (testing "Create JSON response - custom headers not supported directly"
    (let [data {:message "test"}
          response (mw/json-response data 200)]
      ;; json-response only takes body and status, not custom headers
      (is (= 200 (:status response)))
      (is (= "application/json" (get-in response [:headers "Content-Type"]))))))

;; =============================================================================
;; Error Message Sanitization Tests
;; =============================================================================

(deftest sanitize-error-message-java-error-test
  (testing "Sanitize messages with .java file reference"
    (let [msg "Error in MyFile.java: Connection failed"
          sanitized (mw/sanitize-error-message msg)]
      (is (string? sanitized))
      (is (= "Internal server error" sanitized)))))

(deftest sanitize-error-message-stack-trace-test
  (testing "Sanitize messages with bracket patterns [stack trace]"
    (let [msg "Error at com.example.Handler:123 [some context]"
          sanitized (mw/sanitize-error-message msg)]
      (is (string? sanitized))
      (is (= "Internal server error" sanitized)))))

(deftest sanitize-error-message-nil-test
  (testing "Handle nil input - sanitize-error-message throws NPE"
    ;; sanitize-error-message doesn't handle nil, it will throw NPE
    (is (thrown? NullPointerException (mw/sanitize-error-message nil)))))

(deftest sanitize-error-message-safe-message-test
  (testing "Pass through safe messages unchanged"
    (let [msg "User not found"
          sanitized (mw/sanitize-error-message msg)]
      (is (= msg sanitized)))))

(deftest sanitize-error-message-empty-test
  (testing "Handle empty string"
    (let [sanitized (mw/sanitize-error-message "")]
      (is (string? sanitized)))))

;; =============================================================================
;; Exception Handling Tests
;; =============================================================================

(deftest wrap-exception-generic-exception-test
  (testing "Catch and return sanitized error response"
    (let [handler (fn [req] (throw (Exception. "Database connection failed")))
          middleware (mw/wrap-exception handler)
          request {}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 500 (:status response)))
      (is (= "application/json" (get-in response [:headers "Content-Type"])))
      (is (= "error" (:status body)))
      (is (string? (:error body)))
      (is (contains? body :timestamp)))))

(deftest wrap-exception-java-error-test
  (testing "Pass through safe error messages unchanged"
    (let [msg "Database connection failed"
          handler (fn [req] (throw (Exception. msg)))
          middleware (mw/wrap-exception handler)
          request {}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 500 (:status response)))
      (is (= msg (:error body))))))

(deftest wrap-exception-success-pass-through-test
  (testing "Pass through successful responses"
    (let [handler (fn [req] {:status 200 :body "success"})
          middleware (mw/wrap-exception handler)
          request {}
          response (middleware request)]
      (is (= 200 (:status response)))
      (is (= "success" (:body response))))))

;; =============================================================================
;; Validation Error Tests
;; =============================================================================

(deftest wrap-validation-error-exception-info-test
  (testing "Handle validation errors with custom message"
    (let [handler (fn [req] (throw (ex-info "Validation failed" {:error "Invalid UUID"})))
          middleware (mw/wrap-validation-error handler)
          request {}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 400 (:status response)))
      (is (= "validation-error" (:status body)))
      (is (= "Invalid UUID" (:error body))))))

(deftest wrap-validation-error-default-message-test
  (testing "Use default message when error key not present"
    (let [handler (fn [req] (throw (ex-info "Validation failed" {})))
          middleware (mw/wrap-validation-error handler)
          request {}
          response (middleware request)
          body (parse-json-body response)]
      (is (= 400 (:status response)))
      (is (= "Validation failed" (:error body))))))

(deftest wrap-validation-error-pass-through-test
  (testing "Pass through successful responses"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-validation-error handler)
          request {}
          response (middleware request)]
      (is (= 200 (:status response))))))

;; =============================================================================
;; CORS Middleware Tests
;; =============================================================================

(deftest wrap-cors-allowed-origin-test
  (testing "Add CORS headers for allowed origin"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-cors handler)
          request {:headers {"origin" "http://localhost:3000"}}
          response (middleware request)]
      (is (= "http://localhost:3000" (get-in response [:headers "Access-Control-Allow-Origin"])))
      (is (= "true" (get-in response [:headers "Access-Control-Allow-Credentials"])))
      (is (some? (get-in response [:headers "Access-Control-Allow-Methods"])))
      (is (some? (get-in response [:headers "Access-Control-Allow-Headers"]))))))

(deftest wrap-cors-disallowed-origin-test
  (testing "Do not add Access-Control-Allow-Origin for disallowed origin"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-cors handler)
          request {:headers {"origin" "http://evil.com"}}
          response (middleware request)]
      (is (= "" (get-in response [:headers "Access-Control-Allow-Origin"])))
      (is (some? (get-in response [:headers "Access-Control-Allow-Methods"]))))))

(deftest wrap-cors-no-origin-header-test
  (testing "Handle request without origin header"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-cors handler)
          request {:headers {}}
          response (middleware request)]
      (is (= "" (get-in response [:headers "Access-Control-Allow-Origin"]))))))

;; =============================================================================
;; Security Headers Tests
;; =============================================================================

(deftest wrap-security-headers-test
  (testing "Add security headers to response"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-security-headers handler)
          request {}
          response (middleware request)]
      (is (= "nosniff" (get-in response [:headers "X-Content-Type-Options"])))
      (is (= "DENY" (get-in response [:headers "X-Frame-Options"])))
      (is (= "1; mode=block" (get-in response [:headers "X-XSS-Protection"])))
      (is (some? (get-in response [:headers "Strict-Transport-Security"])))
      (is (some? (get-in response [:headers "Content-Security-Policy"])))
      (is (= "strict-origin-when-cross-origin" (get-in response [:headers "Referrer-Policy"]))))))

(deftest wrap-security-headers-merge-existing-test
  (testing "Merge with existing headers"
    (let [handler (fn [req] {:status 200
                             :body "OK"
                             :headers {"X-Custom" "value"}})
          middleware (mw/wrap-security-headers handler)
          request {}
          response (middleware request)]
      (is (= "value" (get-in response [:headers "X-Custom"])))
      (is (some? (get-in response [:headers "X-Content-Type-Options"]))))))

;; =============================================================================
;; Request ID Tests
;; =============================================================================

(deftest wrap-request-id-test
  (testing "Add unique request ID to response"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-request-id handler)
          request1 {}
          response1 (middleware request1)
          request2 {}
          response2 (middleware request2)]
      (is (some? (get-in response1 [:headers "X-Request-ID"])))
      (is (some? (get-in response2 [:headers "X-Request-ID"])))
      ;; Request IDs should be unique
      (is (not= (get-in response1 [:headers "X-Request-ID"])
                (get-in response2 [:headers "X-Request-ID"]))))))

(deftest wrap-request-id-format-test
  (testing "Request ID is valid UUID format"
    (let [handler (fn [req] {:status 200 :body "OK"})
          middleware (mw/wrap-request-id handler)
          request {}
          response (middleware request)
          request-id (get-in response [:headers "X-Request-ID"])]
      ;; Should parse as UUID (no exception thrown)
      (is (some? (UUID/fromString request-id))))))

;; =============================================================================
;; Content Negotiation Tests
;; =============================================================================

(deftest wrap-content-type-default-test
  (testing "Default to application/json when Accept header missing"
    (let [handler (fn [req] {:status 200 :body (:accept req)})
          middleware (mw/wrap-content-type handler)
          request {:headers {}}
          response (middleware request)]
      (is (= "application/json" (:body response))))))

(deftest wrap-content-type-custom-test
  (testing "Use Accept header value when present"
    (let [handler (fn [req] {:status 200 :body (:accept req)})
          middleware (mw/wrap-content-type handler)
          request {:headers {"accept" "application/xml"}}
          response (middleware request)]
      (is (= "application/xml" (:body response))))))

;; =============================================================================
;; Response Helpers Tests
;; =============================================================================

(deftest success-response-default-test
  (testing "Create success response with default 200 status"
    (let [data {:message "OK"}
          response (mw/success-response data)
          body (parse-json-body response)]
      (is (= 200 (:status response)))
      (is (= "application/json" (get-in response [:headers "Content-Type"])))
      (is (= true (:success body)))
      (is (= data (:data body))))))

(deftest success-response-custom-status-test
  (testing "Create success response with custom status"
    (let [data {:message "Created"}
          response (mw/success-response data 201)
          body (parse-json-body response)]
      (is (= 201 (:status response)))
      (is (= true (:success body)))
      (is (= data (:data body))))))

(deftest error-response-default-test
  (testing "Create error response with default 500 status"
    (let [msg "Internal error"
          response (mw/error-response msg)
          body (parse-json-body response)]
      (is (= 500 (:status response)))
      (is (= false (:success body)))
      (is (= msg (:error body)))
      (is (contains? body :timestamp)))))

(deftest error-response-custom-status-test
  (testing "Create error response with custom status"
    (let [msg "Not found"
          response (mw/error-response msg 404)
          body (parse-json-body response)]
      (is (= 404 (:status response)))
      (is (= false (:success body)))
      (is (= msg (:error body))))))

(deftest paginated-response-test
  (testing "Create paginated response"
    (let [data [{:id 1} {:id 2}]
          total 100
          page 1
          per-page 10
          response (mw/paginated-response data total page per-page)
          body (parse-json-body response)]
      (is (= 200 (:status response)))
      (is (= true (:success body)))
      (is (= data (:data body)))
      (is (= total (:total (:pagination body))))
      (is (= page (:page (:pagination body))))
      (is (= per-page (:per-page (:pagination body))))
      ;; total-pages is a float due to Math/ceil
      (is (= 10.0 (:total-pages (:pagination body)))))))

(deftest paginated-response-calculates-total-pages-test
  (testing "Calculate total pages correctly"
    (let [data []
          total 25
          page 10
          per-page 10
          response (mw/paginated-response data total page per-page)
          body (parse-json-body response)]
      ;; total-pages is a float due to Math/ceil
      (is (= 3.0 (:total-pages (:pagination body)))))))

(deftest paginated-response-empty-page-test
  (testing "Handle empty page"
    (let [data []
          total 0
          page 1
          per-page 10
          response (mw/paginated-response data total page per-page)
          body (parse-json-body response)]
      ;; total-pages is a float due to Math/ceil (0.0 for 0 total)
      (is (= 0.0 (:total-pages (:pagination body)))))))

;; =============================================================================
;; Content-Type Header Tests
;; =============================================================================

(deftest json-response-content-type-test
  (testing "JSON response has correct Content-Type header"
    (let [response (mw/json-response {:test "data"})]
      (is (= "application/json" (get-in response [:headers "Content-Type"]))))))
