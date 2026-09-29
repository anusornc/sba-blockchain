(ns datomic-blockchain.api.test-routes
  "Route-level tests on the real route table: path precedence and the
   auth wrappers (the role inherited from the removed twin table)."
  (:require [clojure.test :refer :all]
            [clojure.data.json :as json]
            [ring.mock.request :as mock]
            [datomic-blockchain.api.handlers.traceability :as trace-handlers]
            [datomic-blockchain.api.routes :as routes]
            [datomic-blockchain.test-support :as ts]))

(use-fixtures :each (fn [f] (f) (ts/retire-all!)))

(defn- app
  "The real table with the full middleware stack."
  []
  (routes/create-handler (ts/fresh-conn) nil))

(defn- parse-body [response]
  (json/read-str (:body response) :key-fn keyword))

(deftest trace-id-route-is-not-shadowed-by-public-qr-route-test
  (testing "authenticated trace IDs are not captured by the public QR route"
    (with-redefs [trace-handlers/handle-trace-by-qr (fn [_ _] {:status 200 :body "qr"})]
      (let [response ((app) {:request-method :get
                             :uri "/api/trace/not-a-real-id"
                             :headers {}})]
        (is (= 401 (:status response)))
        (is (re-find #"Missing authorization" (:body response))))))
  (testing "the public QR lookup remains available on its explicit path"
    (with-redefs [trace-handlers/handle-trace-by-qr (fn [_ _] {:status 200 :body "qr"})]
      (is (= 200 (:status ((app) {:request-method :get
                                  :uri "/api/trace/qr/QR-123"
                                  :headers {}})))))))

(deftest require-auth-wrapper-test
  (testing "missing token is a 401 in the standard error envelope"
    (let [response ((app) (mock/request :get "/api/trace/550e8400-e29b-41d4-a716-446655440000"))
          body (parse-body response)]
      (is (= 401 (:status response)))
      (is (= false (:success body)))
      (is (re-find #"Missing authorization" (:error body)))))
  (testing "invalid token is a 401 in the standard error envelope"
    (let [response ((app) (-> (mock/request :get "/api/trace/550e8400-e29b-41d4-a716-446655440000")
                              (mock/header "authorization" "Bearer not-a-real-token")))
          body (parse-body response)]
      (is (= 401 (:status response)))
      (is (= false (:success body)))
      (is (re-find #"Invalid token" (:error body))))))

(deftest require-admin-wrapper-test
  (testing "a valid non-admin token is refused with 403"
    (let [token (routes/generate-auth-token "user-1" {:roles [:user]})
          response (routes/require-admin (constantly {:status 200 :body "ok"})
                                         {:headers {"authorization" (str "Bearer " token)}})
          body (parse-body response)]
      (is (= 403 (:status response)))
      (is (= false (:success body)))
      (is (re-find #"Admin" (:error body)))))
  (testing "an admin token reaches the handler"
    (let [token (routes/generate-auth-token "admin-1" {:roles [:admin]})
          response (routes/require-admin (constantly {:status 200 :body "ok"})
                                         {:headers {"authorization" (str "Bearer " token)}})]
      (is (= 200 (:status response))))))

(deftest unknown-route-is-a-404-envelope-test
  (testing "the fallback 404 uses the standard error envelope"
    (let [response ((app) (mock/request :get "/api/no-such-endpoint"))
          body (parse-body response)]
      (is (= 404 (:status response)))
      (is (= false (:success body))))))
