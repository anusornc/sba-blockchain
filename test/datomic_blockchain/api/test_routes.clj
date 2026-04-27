(ns datomic-blockchain.api.test-routes
  "Route-level regression tests for API path precedence."
  (:require [clojure.test :refer :all]
            [datomic-blockchain.api.handlers.core :as handlers]
            [datomic-blockchain.api.handlers.graph :as graph-handlers]
            [datomic-blockchain.api.handlers.traceability :as trace-handlers]
            [datomic-blockchain.api.routes :as routes]
            [datomic-blockchain.api.routes-v2 :as routes-v2]))

(deftest trace-id-route-is-not-shadowed-by-public-qr-route-test
  (testing "v1 authenticated trace IDs are not captured by the public QR route"
    (with-redefs [handlers/handle-trace-by-qr (fn [_] {:status 200 :body "qr"})]
      (let [response (routes/api-routes {:request-method :get
                                         :uri "/api/trace/not-a-real-id"
                                         :headers {}})]
        (is (= 401 (:status response)))
        (is (re-find #"Missing authorization" (:body response))))))
  (testing "v1 public QR lookup remains available on the explicit QR path"
    (with-redefs [handlers/handle-trace-by-qr (fn [_] {:status 200 :body "qr"})]
      (is (= 200 (:status (routes/api-routes {:request-method :get
                                              :uri "/api/trace/qr/QR-123"
                                              :headers {}})))))))

(deftest trace-id-route-is-not-shadowed-by-public-qr-route-v2-test
  (testing "v2 authenticated trace IDs are not captured by the public QR route"
    (with-redefs [trace-handlers/handle-trace-by-qr (fn [_ _] {:status 200 :body "qr"})]
      (let [response (routes-v2/api-routes {:request-method :get
                                            :uri "/api/trace/not-a-real-id"
                                            :headers {}})]
        (is (= 401 (:status response)))
        (is (re-find #"Missing authorization" (:body response))))))
  (testing "v2 public QR lookup remains available on the explicit QR path"
    (with-redefs [trace-handlers/handle-trace-by-qr (fn [_ _] {:status 200 :body "qr"})]
      (is (= 200 (:status (routes-v2/api-routes {:request-method :get
                                                 :uri "/api/trace/qr/QR-123"
                                                 :headers {}})))))))

(deftest routes-v2-stats-uses-optional-auth-with-correct-arity-test
  (testing "v2 /api/stats does not call optional-auth with an extra dependency arg"
    (with-redefs [graph-handlers/handle-get-stats (fn [_ _] {:status 200 :body "stats"})]
      (is (= 200 (:status (routes-v2/api-routes {:request-method :get
                                                 :uri "/api/stats"
                                                 :headers {}})))))))
