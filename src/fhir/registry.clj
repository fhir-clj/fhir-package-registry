(ns fhir.registry
  (:require
   [system]
   [http]
   [pg]
   [pg.repo]
   [cheshire.core]
   [uui]
   [uui.heroicons :as ico]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [ring.middleware.multipart-params :as multipart] )
  (:import
   [java.util Base64]
   [java.net URL]
   [java.io InputStream]
   [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
   [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]))

(system/defmanifest
  {:description "FHIR Registry"
   :deps ["pg" "pg.repo" "http"]
   :config {:history {:type "boolean"}}})

(def current-ns *ns*)

(defn ^{:http {:path "/"}}
  index
  [context request]
  (let [packages   (pg.repo/select context {:table "fhir_packages.package"})]
    (uui/boost-response
     context request
     [:div {:class "divide-x"}
      (for [pkg packages]
        [:div {:class "py-2"}
         [:a {:href (str "/" (:name pkg))}
          [:b (:name pkg)]]
         #_(uui/json-block pkg)])])))

(defn ^{:http {:path "/:package"}}
  package
  [context {{package :package} :route-params :as request}]
  (let [package   (pg.repo/read context {:table "fhir_packages.package" :match {:name package}})]
    {:body package :status 200}
    #_(uui/boost-response
     context request
     [:div {:class "divide-x"}
      (uui/json-block package)])))

(defn ^{:http {:path "/:package/:version"}}
  package-version
  [context request]
  {:status 404})

(defn reduce-tar [^InputStream input-stream cb & [acc]]
  (with-open [^GzipCompressorInputStream gzip-compressor-input-stream (GzipCompressorInputStream. input-stream)
              ^TarArchiveInputStream tar-archive-input-stream (TarArchiveInputStream. gzip-compressor-input-stream)]
    (loop [acc (or acc {})]
      (if-let [^TarArchiveEntry entry (.getNextTarEntry tar-archive-input-stream)]
        (let [^String nm (str/replace (.getName entry) #"package/" "")
              read-fn (fn [& [json]]
                        (let [content (byte-array (.getSize entry))]
                          (.read tar-archive-input-stream content)
                          (if json
                            (cheshire.core/parse-string (String. content) keyword)
                            (String. content))))]
          (recur (cb acc nm read-fn)))
        acc))))

(defn decode-base64-to-file
  "Decodes a base64 string and writes it to a file.
   Returns {:success true, :file path} on success or
   {:success false, :error message} on failure."
  [base64-string output-file-path]
  (try
    (let [decoder (Base64/getDecoder)
          decoded-bytes (.decode decoder base64-string)]
      (with-open [out (io/output-stream output-file-path)]
        (.write out decoded-bytes))
      {:success true, :file output-file-path})
    (catch Exception e
      (println (.getMessage e)))))

(defn ^{:http {:path "/:package" :method :put}}
  publish-package
  [context {{pkg :package} :route-params :as request}]
  (let [package (cheshire.core/parse-stream (io/reader (:body request)) keyword)
        package-file (str "packages/" (:name package) ".json")
        prev-package (when (.exists (io/file package-file))
                       (cheshire.core/parse-string (slurp package-file) keyword))
        package-resource (merge prev-package (dissoc package :_attachments))]
    (spit package-file (cheshire.core/generate-string package))
    (pg.repo/upsert context {:table "fhir_packages.package" :resource package-resource})
    (doseq [[k v] (:_attachments package)]
      (let [out-file (str "packages/" (:name package) "/" (name k))]
        (io/make-parents out-file)
        (decode-base64-to-file (:data v) out-file))))
  {:status 200})

;; https://marmelab.com/blog/2022/12/22/how-to-implement-web-login-in-a-private-npm-registry.html
;; https://github.com/HealthIntersections/fhirserver/blob/master/server/package_spider.pas
;; https://fhir.github.io/ig-registry/package-feeds.json
;; https://github.com/HealthIntersections/fhirserver/blob/master/server/package_spider.pas
(defn ^{:http {:path "/-/v1/login" :method :post}}
  login
  [context request]
  (println :login (slurp (:body request)))
  {:status 200
   :body {:loginUrl "http://localhost:3333/login"
          :doneUrl  "http://localhost:3333/login/done"}})

(defn ^{:http {:path "/login/done" :method :get}}
  login-done
  [context request]
  {:status 200
   :body {:token "token"}})

(defn ^{:http {:path "/-/user/:login" :method :put}}
  adduser
  [context request]
  {:status 200
   :body {}})

(system/defstart
  [context config]
  (pg/migrate-prepare context)
  (pg/migrate-up context)
  (http/register-ns-endpoints context current-ns)
  {})

(def default-config
  {:services ["pg" "pg.repo" "http" "uui" "fhir.registry"]
   :http {:port 3333
          :max-body 108388608}})

(comment
  (require '[pg.docker :as pgd])
  (require '[system.dev :as dev])

  (dev/update-libs)

  (pgd/delete-pg "fhir-registry")

  (pg/generate-migration "fhir_packages")

  (def pg-config (pgd/ensure-pg "fhir-registry"))

  (def context (system/start-system (assoc default-config :pg pg-config)))

  (system/stop-system context)

  (pg/migrate-up context "fhir_packages")

  (pg.repo/select context {:table "fhir_packages.package"})

  )
