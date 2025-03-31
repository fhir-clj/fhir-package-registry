;; logic to sync changes and reindex packages in bucket
(ns fhir.registry.index
  (:require
   [system]
   [http]
   [pg]
   [pg.repo]
   [pg.docker :as pgd]
   [cheshire.core]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set]
   [clojure.walk]
   [fhir.registry.gcs :as gcs]
   [fhir.registry.legacy]
   [org.httpkit.client]
   [fhir.registry.semver :as semver]
   [fhir.registry.ndjson :as ndjson]
   [fhir.registry.packages2 :as packages2]
   [fhir.registry.tar :as tar])
  (:import
   [java.util Base64]
   [java.net URL]
   [java.io InputStream]
   [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
   [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]))


(defn map-indexed-starting [f start-from coll]
  (map-indexed (fn [idx item] (f (+ idx start-from) item)) coll))

(defn remove-nils [m]
  (clojure.walk/postwalk
   (fn [x]
     (if (map? x)
       (into {} (filter (fn [[_ v]] (some? v)) x))
       x))
   m))

(defn format-package [{v :version :as package}]
  (assoc (remove-nils package)
         :_id (str (:name package) "@" (:version package))
         :dist {:tarball (str "http://fs.get-ig.org/-/" (:name package) "-" (:version package) ".tgz")}))

(defn build-package-json [versions]
  (when (seq versions)
    (let [sorted-versions (sort semver/semver-comparator (keys versions))
          latest-version (last sorted-versions)
          latest (get versions latest-version)]
      (assoc latest
             :_versions (reverse sorted-versions)
             :versions versions
             :dist-tags {:latest latest-version}))))

(system/defmanifest
  {:description "FHIR Registry Indexer"
   :deps []
   :config {:history {:type "boolean"}}})

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

(defn load-from-url-pacakge2 [context file-name]
  (time
   (with-open [inps (.openStream (java.net.URL. (str "http://packages2.fhir.org/web/" file-name)))
               outs  (fhir.registry.gcs/output-stream context fhir.registry.gcs/DEFAULT_BUCKET (str "-/" file-name))]
     (io/copy inps outs))))

(defn load-from-simplifier [context pkg-name ]
  (if-let [pkg (fhir.registry.legacy/package-info pkg-name)]
    (with-open [inps (.openStream (java.net.URL. (:tarball (:dist pkg))))
                outs  (fhir.registry.gcs/output-stream context fhir.registry.gcs/DEFAULT_BUCKET (str "-/" (:name pkg) "-" (:version pkg) ".tgz"))]
      (io/copy inps outs))
    (throw (Exception. (str "No " pkg-name)))))

(defn load-from-simplifier-to-file [pkg-name file-name]
  (if-let [pkg (fhir.registry.legacy/package-info pkg-name)]
    (let [inps (.openStream (java.net.URL. (:tarball (:dist pkg))))]
      (with-open [outps (io/output-stream (io/file file-name))]
        (io/copy inps outps)))
    (throw (Exception. (str "No " pkg-name)))))

(defn list-tgz [context]
  (->> (gcs/lazy-objects context gcs/DEFAULT_BUCKET "-/")
       (filter (fn [x] (str/ends-with? (.getName x) ".tgz")))
       (mapv (fn [x] (bean x)))
       (sort-by :size)
       (mapv (fn [x]
               {:size (:size x)
                :name (clojure.string/replace (:name x) #"-/" "")}))))

(defn diff-with-packages2 [context]
  (clojure.set/difference
   (into #{} (packages2/packages))
   (into #{} (map :name (list-tgz context)))))

(defn sync-with-package2 [context]
  (let [diff (diff-with-packages2 context)]
    (when (empty? diff)
      (system/info context ::packages2 "Nothing to sync from packages2"))
    (->> diff
         (pmap (fn [x]
                 (system/info context ::packages2 (str "load " x " from packages2"))
                 (time (load-from-url-pacakge2 context x)))))))

(defn load-ndjson [context file-name]
  ;; (println file-name)
  (let [package (str/replace file-name #"(^-/|.ndjson.gz$)" "")
       [package-name package-version] (str/split package #"-" 2)]
   (pg.repo/load
    context {:table "fhir_packages.canonical"}
    (fn [write]
      (fhir.registry.ndjson/read-stream
       (gcs/input-stream context gcs/DEFAULT_BUCKET file-name)
       (fn [_ res _line-num]
         (let [res (assoc res
                          :id (java.util.UUID/randomUUID)
                          :package_name package-name
                          :package_version package-version)]
           (write res))
         :ok))))))

(defn load-canonicals [context]
  (let [context (system/ctx-set-log-level context :error)]
    (time
     (->> (gcs/lazy-objects context gcs/DEFAULT_BUCKET "-/")
          (filter (fn [x] (str/ends-with? (.getName x) ".ndjson.gz")))
          (pmap (fn [x]
                 (try
                   (load-ndjson context (.getName x))
                   (print ".") (flush)
                   (catch Exception e
                     (println :ERROR (.getMessage e))))))
          (doall)))))

(defn current-packages-from-tgz
  "this is expensive operation"
  [context]
  (time
   (->>
    (gcs/lazy-objects context gcs/DEFAULT_BUCKET "-/")
    (filter (fn [x] (str/ends-with? (.getName x) ".tgz")))
    (pmap
     (fn [x]
       (when-let [s (tar/find-entry-input (gcs/blob-input-stream x) "package.json")]
         (let [package (remove-nils (cheshire.core/parse-string s keyword))]
           (assoc package :tgz (.getName x))))))
    (filter identity)
    (into []))))

(defn current-packages [context]
  (fhir.registry.ndjson/read-stream
   (gcs/input-stream context gcs/DEFAULT_BUCKET "pgks.ndjson.gz")))

(defn write-current-packages [context pkgs]
  (fhir.registry.ndjson/write-stream-ndjson-gz
   (gcs/output-stream context gcs/DEFAULT_BUCKET "pgks.ndjson.gz")
   (fn [write]
     (->> pkgs
          (mapv (fn [x] (-> x (dissoc :_id :dist) (assoc :tgz (str "-/" (:name x) "-" (:version x) ".tgz")))))
          (sort-by (fn [x] (:tgz x)))
          (mapv (fn [x] (write (cheshire.core/generate-string x))))))))


(defn index-new-packages [context new-pkgs-idx pkg-idx]
  (->> new-pkgs-idx
       (pmap (fn [[pkg-name pkg-versions]]
               (when-let [versions (mapv format-package (get pkg-idx pkg-name))]
                 (let [pkgv (build-package-json (->> (concat versions (mapv format-package pkg-versions))
                                                     (reduce (fn [acc {v :version :as pkg}]
                                                               (println pkg)
                                                               (assoc acc (or v "0.0.0") (assoc pkg :version (or v "0.0.0"))))
                                                             {})))]
                   (println :write (str "pkgs/" pkg-name))
                   (gcs/spit-blob context (str "pkgs/" pkg-name)
                                  (cheshire.core/generate-string pkgv {:pretty true}) {:content-type "application/json"})))))
       (doall)))

(defn update-feed [context new-packages]
  (let [feed (gcs/read-feed context)
        lsn (:lsn (last feed))
        new-packages-with-lsn (->> new-packages
                                   (sort-by :name)
                                   (map-indexed-starting
                                    (fn [idx pkg]
                                      (assoc pkg
                                             :lsn idx
                                             :timestamp (java.time.Instant/now)
                                             :tgz (str "-/" (:name pkg) "-" (:version pkg) ".tgz")))
                                    (inc (or lsn -1)))
                                   (doall))]
    (println :update/feed :from lsn)
    (fhir.registry.ndjson/write-stream-ndjson-gz
     (gcs/output-stream context gcs/DEFAULT_BUCKET "feed.ndjson.gz")
     (fn [write]
       (->> feed (mapv (fn [x] (write (cheshire.core/generate-string x)))))
       (->> new-packages-with-lsn (mapv (fn [x] (write (cheshire.core/generate-string x)))))))))

(defn read-package-from-tgz [context tgz]
  (let [inps (gcs/input-stream context gcs/DEFAULT_BUCKET (str "-/" tgz))]
    (cheshire.core/parse-string (tar/find-entry-input inps "package.json") keyword)))

(defn reindex-tgz [context]
  (let [cur-pgs (current-packages context)
        pkg-idx (->> cur-pgs (group-by :name))
        tgzs (list-tgz context)
        _  (println :current-packages (count cur-pgs) :current-tgzs (count tgzs))
        diff (clojure.set/difference
              (into #{} (mapv :name tgzs))
              (->> (mapv :tgz cur-pgs)
                   (mapv (fn [x] (str/replace x #"-/" "")))
                   (into #{})))
        new-packages (->> diff (pmap #(read-package-from-tgz context %)) (doall))
        new-pkgs-idx (group-by :name new-packages)]
    (when (empty? new-packages)
      (system/info context ::index-tgz "Nothing to index"))
    (when-not (empty? new-packages)
      (println :update/current-packages (mapv :name new-packages))
      (index-new-packages context new-pkgs-idx pkg-idx)
      (write-current-packages context (->> (concat cur-pgs new-packages) (sort-by (fn [x] [(:name x) (:version x)]))))
      (update-feed context new-packages))))
;; OBSOLETE
(defn load-from-url-pacakge2 [context file-name]
  (time
   (with-open [inps (.openStream (java.net.URL. (str "http://packages2.fhir.org/web/" file-name)))
               outs  (fhir.registry.gcs/output-stream context fhir.registry.gcs/DEFAULT_BUCKET (str "-/" file-name))]
     (io/copy inps outs))))

(defn load-from-simplifier [context pkg-name ]
  (if-let [pkg (fhir.registry.legacy/package-info pkg-name)]
    (with-open [inps (.openStream (java.net.URL. (:tarball (:dist pkg))))
                outs  (fhir.registry.gcs/output-stream context fhir.registry.gcs/DEFAULT_BUCKET (str "-/" (:name pkg) "-" (:version pkg) ".tgz"))]
      (io/copy inps outs))
    (throw (Exception. (str "No " pkg-name)))))

(defn load-from-simplifier-to-file [pkg-name file-name]
  (if-let [pkg (fhir.registry.legacy/package-info pkg-name)]
    (let [inps (.openStream (java.net.URL. (:tarball (:dist pkg))))]
      (with-open [outps (io/output-stream (io/file file-name))]
        (io/copy inps outps)))
    (throw (Exception. (str "No " pkg-name)))))

(defn re-index [context]
  (time
   (do
     (pg/execute! context {:sql "truncate fhir_packages.package"})
     (pg/execute! context {:sql "truncate fhir_packages.package_dependency"})
     (->> (gcs/list-packages context)
          (pmap (fn [blob]
                  (let [res (gcs/read-json-blob blob)]
                    (pg.repo/upsert context {:table "fhir_packages.package"
                                             :resource (assoc res :id (str (:name res) "@" (:version res)))})
                    (doseq [[d v] (:dependencies res)]
                      (pg.repo/upsert context {:table "fhir_packages.package_dependency"
                                               :resource {:id (str (:name res) "@" (:version res) "->" (name d) "@" v)
                                                          :source_name (:name res)
                                                          :source_version (:version res)
                                                          :destination_name (name d)
                                                          :destination_version v}})))))))))

(defn start-periodic-job [job-name period-ms f]
  (let [thread (Thread.
                (fn [] (loop []
                        (try (f) (catch Exception e (println ::job-exception (.getMessage e))))
                        (Thread/sleep period-ms) (recur))) job-name)]
    (.start thread)))

(defn stop-periodic-job [job-name]
  (->> (Thread/getAllStackTraces)
       keys
       (filter #(= (.getName %) job-name))
       (mapv (fn [thr] (.interrupt thr)))))

(defn index-resources [context filename]
  (time
   (let [inps (gcs/input-stream context gcs/DEFAULT_BUCKET (str "-/" filename ".tgz"))
         out-file (str "rs/" filename ".ndjson.gz")]
     (println :sync out-file)
     (fhir.registry.ndjson/write-stream-ndjson-gz
      (gcs/output-stream context gcs/DEFAULT_BUCKET out-file)
      (fn [write]
        (try
          (reduce-tar inps (fn [acc file read]
                             (if (str/ends-with? file ".json")
                               (try
                                 (let [res (cheshire.core/parse-string (read))]
                                   (if (and (get res "url") (get res "resourceType"))
                                     (do #_(println file (get res "url") (get res "resourceType") (get res "kind") (get res "type"))
                                         (write (cheshire.core/generate-string (assoc (dissoc res "text") "_filename" file)))
                                         (assoc acc file (select-keys res ["url" "resourceType" "kind"])))
                                     acc))
                                 (catch Exception e
                                   (println file (.getMessage e))
                                   acc))
                               acc)))
          (catch Exception e
            (println :tar-error filename (.getMessage e)))))))))


(defn update-resources [context]
  (let [tgzs (->> (gcs/lazy-objects context gcs/DEFAULT_BUCKET "-/")
                  (mapv (fn [x] (str/replace (.getName x) #"(^-/|\.tgz$)" "")))
                  (into #{}))
        rss (->> (gcs/lazy-objects context gcs/DEFAULT_BUCKET "rs/")
                 (mapv (fn [x] (str/replace (.getName x) #"(^rs/|\.ndjson\.gz$)" "")))
                 (into #{}))
        diff (->> (clojure.set/difference tgzs  rss)
                  (filter #(not (str/blank? %))))]
    (when (empty? diff)
      (system/info context ::packages2 "Nothing to ndjson"))
    (->> diff
         (pmap (fn [s] (println :index s) (index-resources context s))))))

(defn hard-update-resources [context]
  (->> (gcs/lazy-objects context gcs/DEFAULT_BUCKET "-/")
       (pmap (fn [x]
               (let [file (str/replace (.getName x) #"(^-/|\.tgz$)" "")]
                 (index-resources context file)
                 (print ".") (flush))))
       (doall)))


(system/defstart
  [context config]
  (println ::start)
  (start-periodic-job "packages2-sync" (* 5 60 1000) (fn [] (sync-with-package2 context)))
  (start-periodic-job "index-tgz" (* 5 60 1000) (fn [] (reindex-tgz context)))
  (start-periodic-job "index-resources" (* 5 60 1000) (fn [] (update-resources context)))
  {:jobs ["pacakges2-sync" "index-tgz" "index-resources"]})

(system/defstop
  [context state]
  (doseq [j (:jobs state)]
    (println :stop j) (stop-periodic-job j)))

(comment
  (def context fhir.registry/context)

  (def current-pkgs (current-packages context))

  (def pkgs2 (packages2/packages))

  (sync-with-package2 context)

  (reindex-tgz context)
  ;; reindex ndjsons
  ;; move sync code into namespace
  ;; hard packages reindex

  (start-periodic-job "sync" 1000 (fn [] (print ".") (flush)))

  (stop-periodic-job "sync")

  (start context {})
  (stop context {:jobs ["pacakges2-sync" "index-tgz" "index-resources"]})


  (->> (Thread/getAllStackTraces) keys (mapv #(.getName %)))

  (stop-periodic-job "packages2-sync")
  (stop-periodic-job "index-tgz")
  (stop-periodic-job "index-resources")

  (update-resources context)



  )

