(ns fhir.registry.legacy
  (:require [org.httpkit.client :as http]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [cheshire.core]
            [clojure.java.io :as io])
  (:import
   [java.net URL]
   [java.io InputStream]))

;; https://packages.simplifier.net/<pgk-name>/-/<pk-name>-<pkg-version>.tgz

(def SIMPLIFIER_REPO "https://packages.simplifier.net")

(defn package-info
  "get package information pkg could be just name a.b.c or versioned name a.b.c@1.0.0"
  [pkg & [registry]]
  (let [[pkg version] (str/split pkg #"@" 2)
        body (slurp (URL. (str (or registry SIMPLIFIER_REPO) "/" pkg)))]
    (try
      (-> body
          (cheshire.core/parse-string keyword)
          (cond-> version
            (get-in [:versions (keyword version)])))
      (catch Exception e
        (println ::error body)
        (throw e)))))

(defn package-url [name version]
  (str "https://packages.simplifier.net/" name "/-/" name "-" version ".tgz"))

(defn package2-url [name version]
  (str "https://packages2.fhir.org/packages/" name "/" version))

(defn save-input-stream-to-file
  [input-stream output-file]
  (io/make-parents output-file)
  (try
    (with-open [in input-stream out (io/output-stream output-file)]
      (io/copy in out)
      {:success true :file output-file})
    (catch Exception e
      {:success false :error (.getMessage e)})))

(defn file-exists? [path]
  (.exists (io/file path)))

(defn mirror-package [name version & [registry]]
  (let [registry (or registry "http://localhost:3333")]
    (if (= 200 (:status @(http/get (str registry "/" name "/" version))))
      :already-installed
      (let [file-name (str "./fhir-packages/" name "-" version ".tgz")]
        (when-not (file-exists? file-name)
          (let [url (package-url name version)
                res @(http/get url)]
            (if (= 200 (:status res))
              (save-input-stream-to-file (:body res) file-name)
              (let [url (package2-url name version)
                    res @(http/get url)]
                (if (= 200 (:status res))
                  (save-input-stream-to-file (:body res) file-name)
                  (throw (Exception. (str name "@" version " not found"))))))))
        (let [res (sh "bash" "-c" (str "npm publish --verbose " file-name  " --registry " registry " --access public"))]
          (when-not (= 0 (:exit res))
            (throw (Exception. (:err res)))))))))


(comment
  (mirror-package "us.nlm.vsac" "0.21.0")
  (mirror-package "hl7.fhir.r4.core" "4.0.1")
  (mirror-package "hl7.fhir.r5.core" "5.0.0")
  (mirror-package "hl7.fhir.r4b.core" "4.3.0")

  (package-info "hl7.fhir.r4.core")
  (package-info "hl7.fhir.r5.core")

  (package-info "hl7.fhir.r4b.core")

  (let [pkg (package-info "hl7.fhir.us.core")]
    (for [v (->> (vals (:versions pkg))
                (sort-by :version))]
      (mirror-package (:name v) (:version v))))

  (package-info "hl7.terminology.r4")


  )


