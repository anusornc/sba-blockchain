(ns datomic-blockchain.config
  (:require [aero.core :as a]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

;; ============================================================================
;; Environment Variable Documentation
;; ============================================================================
;; Required Environment Variables:
;;
;; JWT_SECRET (min 64 characters)
;;   - Secret key for JWT token signing/validation
;;   - MUST be set in all environments for security
;;   - Generate with: openssl rand -base64 64
;;   - Example: JWT_SECRET="your-secure-random-64-byte-secret-key-here-minimum-length"
;;
;; DATOMIC_DB_NAME (required in production mode)
;;   - Name of the Datomic database to connect to
;;   - Optional in development (defaults to "blockchain-dev")
;;   - Example: DATOMIC_DB_NAME=blockchain-pro
;;
;; PRODUCTION_MODE (optional boolean flag)
;;   - When set (any non-empty value), enables production security checks
;;   - Enforces JWT_SECRET requirement and minimum lengths
;;   - Example: PRODUCTION_MODE=true
;;
;; Optional Environment Variables:
;;
;; ALLOWED_ORIGINS (comma-separated list)
;;   - CORS allowed origins for web API
;;   - Example: ALLOWED_ORIGINS=https://example.com,https://app.example.com
;;
;; PORT (overrides server port from config file)
;;   - HTTP port for the API server
;;   - Must be between 1-65535
;;   - Example: PORT=8080

;; ============================================================================
;; Configuration Validation
;; ============================================================================

(def ^:private min-jwt-secret-length 64)

(def ^:private valid-log-levels
  "Set of valid log levels"
  #{:debug :info :warn :error})

(defn- check-env-var
  "Check if environment variable is set and non-empty.
  Returns the value if set, nil otherwise."
  [var-name]
  (let [value (System/getenv var-name)]
    (when (and value (not (str/blank? value)))
      value)))

(defn validate-env-vars!
  "Validate required environment variables.
  Throws ex-info with descriptive message if validation fails."
  [production-mode?]
  (let [errors (atom [])]

    ;; Check JWT_SECRET (required in all modes per security policy)
    (if-let [jwt-secret (check-env-var "JWT_SECRET")]
      (when (< (count jwt-secret) min-jwt-secret-length)
        (swap! errors conj
               (format "JWT_SECRET must be at least %d characters (current: %d)"
                       min-jwt-secret-length (count jwt-secret))))
      (swap! errors conj
             (format "JWT_SECRET environment variable not set. Generate one with: openssl rand -base64 64")))

    ;; Check DATOMIC_DB_NAME (required in production mode)
    (when (and production-mode? (not (check-env-var "DATOMIC_DB_NAME")))
      (swap! errors conj
             "DATOMIC_DB_NAME environment variable must be set in production mode"))

    ;; Throw if any errors found
    (when (seq @errors)
      (throw (ex-info "Configuration validation failed"
                      {:error ::config-validation
                       :errors @errors
                       :production-mode production-mode?})))))

(defn validate-config-values!
  "Validate configuration values are in acceptable ranges.
  Throws ex-info with descriptive message if validation fails."
  [config]
  (let [errors (atom [])]

    ;; Validate server port
    (when-let [port (get-in config [:server :port])]
      (when (or (not (integer? port))
                (< port 1)
                (> port 65535))
        (swap! errors conj
               (format "Server port must be an integer between 1-65535 (got: %s)" port))))

    ;; Validate log level
    (when-let [log-level (get-in config [:log :level])]
      (when (not (contains? valid-log-levels log-level))
        (swap! errors conj
               (format "Log level must be one of %s (got: %s)"
                       (pr-str (sort valid-log-levels))
                       (pr-str log-level)))))

    ;; Validate Datomic config presence
    (when (not (get config :datomic))
      (swap! errors conj "Datomic configuration missing"))

    ;; Throw if any errors found
    (when (seq @errors)
      (throw (ex-info "Configuration values validation failed"
                      {:error ::config-value-validation
                       :errors @errors
                       :config config})))))

(defn validate-config!
  "Comprehensive configuration validation.
  1. Checks required environment variables are set
  2. Validates configuration values are in acceptable ranges
  3. Throws descriptive exceptions if validation fails

  Returns nil if validation passes."
  [config]
  (let [production-mode? (boolean (System/getenv "PRODUCTION_MODE"))]
    ;; Validate environment variables first
    (validate-env-vars! production-mode?)
    ;; Then validate config values
    (validate-config-values! config)
    ;; All validations passed
    nil))

;; ============================================================================
;; Configuration Loading
;; ============================================================================

(defn load-config
  "Load configuration from resources/config/*.edn based on environment.
  Validates configuration before returning.

  Defaults to :dev environment if no profile specified.

  Options:
  - profile: :dev, :test, :prod, or custom profile name

  Environment Variables (see validate-env-vars! for full list):
  - JWT_SECRET: Required (min 64 chars)
  - DATOMIC_DB_NAME: Required in production mode
  - PRODUCTION_MODE: Optional, enables production security checks
  - PORT or SERVER_PORT: Override server port from config file

  Throws:
  - ex-info with :error ::config-validation if env vars invalid
  - ex-info with :error ::config-value-validation if config values invalid"
  ([] (load-config :dev))
  ([profile]
   (let [config-file (io/resource (str "config/" (name profile) ".edn"))
         config (if config-file
                  (a/read-config config-file {:profile profile})
                  (do
                    (log/warn "Config file not found for profile" profile ", using defaults")
                    {:datomic {:db-name "blockchain-dev"
                               :server-type :dev
                               :system "dev"}
                     :server {:port 3000}
                     :log {:level :info}}))]
     ;; Override port from environment variable (PORT or SERVER_PORT)
     (let [port-env (or (check-env-var "PORT")
                        (check-env-var "SERVER_PORT"))
           _ (when port-env (log/debug "Found port environment variable:" port-env))
           updated-config (if port-env
                          (try
                            (let [port (Integer/parseInt port-env)]
                              (log/debug "Setting server port to:" port)
                              (assoc-in config [:server :port] port))
                            (catch NumberFormatException e
                              (log/warn "Invalid PORT or SERVER_PORT environment variable:" port-env)
                              config))
                          config)]
       ;; Validate configuration before returning
       (validate-config! updated-config)
       updated-config))))
