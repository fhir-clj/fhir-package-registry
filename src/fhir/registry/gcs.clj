(ns fhir.registry.gcs
  (:require [clojure.string :as str]
            [system]
            [clojure.java.io :as io]
            [cheshire.core]
            [fhir.registry.ndjson])
  (:import [com.google.cloud.storage StorageOptions
            BlobInfo BlobId
            Storage Bucket Blob Storage$BucketGetOption
            Blob$BlobSourceOption
            Storage$BlobListOption
            Storage$BlobGetOption
            Blob$BlobSourceOption
            Storage$BlobWriteOption]
           [com.google.auth.oauth2 ServiceAccountCredentials]
           [java.util Base64]
           [java.net URL]
           [java.io InputStream]
           [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
           [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStream InputStreamReader BufferedWriter OutputStreamWriter]))

(system/defmanifest
  {:description "google storage for FHIR packages"
   :config {:service-account {:type "string" }}})

(set! *warn-on-reflection* false)

(defn gz-stream [^InputStream str]
  (GZIPInputStream. str))


(comment
  (def svc
    (let [creds (ServiceAccountCredentials/fromStream (io/input-stream "./sa.json"))
          storage (.getService (-> (StorageOptions/newBuilder) (.setCredentials creds) .build))]
      storage))

  )

(defn mk-service [cfg]
  (if-let [sa (:service-account cfg)]
    (let [creds (ServiceAccountCredentials/fromStream (io/input-stream sa))]
      (.getService (-> (StorageOptions/newBuilder) (.setCredentials creds) .build)))
    (.getService (StorageOptions/getDefaultInstance))))

(defn get-svc [context]
  (system/get-system-state context [:svc]))

(defn get-bucket [context bucket-name]
  (let [service (get-svc context)]
    (.get service ^String bucket-name ^"[Lcom.google.cloud.storage.Storage$BucketGetOption;" (into-array Storage$BucketGetOption []))))

(defn objects
  [context ^String bucket & [prefix]]
  (let [bucket (get-bucket context bucket)
        opts (into-array Storage$BlobListOption (cond-> [] prefix  (conj (Storage$BlobListOption/prefix prefix))))
        page   (.list bucket opts)]
    (loop [page page acc (into [] (.getValues page))]
      (if-let [next-page (.getNextPage page)]
        (recur next-page (into acc (.getValues page)))
        (into acc (.getValues page))))))

(defn lazy-page [page]
  (if-let [next-page (.getNextPage page)]
    (lazy-seq (concat (.getValues page) (lazy-page next-page)))
    (.getValues page)))

(defn lazy-objects [context ^String bucket & [prefix]]
  (let [bucket (get-bucket context bucket)
        opts (into-array
              Storage$BlobListOption
              (cond-> [] prefix  (conj (Storage$BlobListOption/prefix prefix))))
        page   (.list bucket opts)]
    (lazy-page page)))

(defn blob-content [^Blob blob & [{json :json}]]
  (let [res (String. (.getContent blob (into-array Blob$BlobSourceOption [])))]
    (if json
      (cheshire.core/parse-string res keyword)
      res)))

(defn blob-input-stream [^Blob blob & [{gz :gzip}]]
  (let [input-stream (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))]
    (if gz
      (gz-stream input-stream)
      input-stream)))

(defn get-blob
  [context bucket file]
  (let [service (get-svc context)
        bid (BlobId/of bucket file)
        blb (.get service bid (into-array Storage$BlobGetOption []))]
    (assert blb (str "FILE NOT EXISTS:" bucket "/" file))
    blb))

(defn input-stream [context bucket file & [gzip]]
  (let [blob (get-blob context bucket file)
        inps (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))]
    (if gzip
      (gz-stream inps)
      inps)))

(defn output-stream [context bucket ^String file  & [{content-type :content-type}]]
  (let [service (get-svc context)
        bid (BlobId/of bucket file)
        binfo (BlobInfo/newBuilder bid)
        _ (when content-type (.setContentType binfo content-type))
        ch (.writer ^Storage service ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))]
    (Channels/newOutputStream ch)))

(def DEFAULT_BUCKET "fs.get-ig.org")

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


(defn list-packages [context]
  (->> (objects context DEFAULT_BUCKET "-")
       (filter (fn [x]
                 (and (str/ends-with? (.getName x) ".json")
                      (not (str/ends-with? (.getName x) ".index.json")))))))

(defn list-tgz [context]
  (->> (objects context DEFAULT_BUCKET "-")
       (filter (fn [x] (str/ends-with? (.getName x) ".tgz")))))

(defn read-json-blob [blob]
  (cheshire.core/parse-string (String. (.getContent blob (into-array Blob$BlobSourceOption []))) keyword) )

(defn reduce-package [context package version cb & [acc]]
  (let [blob (get-blob (get-svc context) DEFAULT_BUCKET (str "-/" package "-" version ".tgz"))]
    (reduce-tar (blob-input-stream blob) cb acc)))

(defn spit-blob [context filename content & [{_content-type :content-type :as opts}]]
  (with-open [w (output-stream context DEFAULT_BUCKET filename opts)]
    (.write w (.getBytes content))))

(defn re-index [context]
  (let [svc (get-svc context)
        idx (->> (objects context DEFAULT_BUCKET "-") (sort-by #(.getName %))
                 (reduce (fn [acc o]
                           (let [file-name (last (str/split (.getName o) #"/"))
                                 _ (println file-name)
                                 ext (last (str/split file-name #"\."))
                                 pkg-name (str/replace file-name #"(\.tgz|\.json)$" "")]
                             (if (and (contains? #{"json" "tgz"} ext) (not (str/ends-with? file-name ".index.json")))
                               (assoc-in acc [pkg-name ext] o)
                               acc)))
                         {}))]
    (time
     (->> idx
          (mapv (fn [[pkg {json "json" tgz "tgz"}]]
                  (when (and tgz (not json))
                    (println :tgz tgz)
                    (try
                      (let [res (time (reduce-tar (blob-input-stream tgz) (fn [acc file read]
                                                                            (cond (= "package.json" file)
                                                                                  (assoc acc :package (read true))
                                                                                  (= ".index.json" file)
                                                                                  (assoc acc :index (read true))
                                                                                  :else acc))))]
                        (when-let [package (:package res)]
                          (println :package (str "-/" pkg ".json"))
                          (with-open [w (output-stream context DEFAULT_BUCKET (str "-/" pkg ".json"))]
                            (.write w (.getBytes (cheshire.core/generate-string package)))))
                        (when-let [index (:index res)]
                          (println :index (str "-/" pkg ".index.json"))
                          (with-open [w (output-stream context DEFAULT_BUCKET (str "-/" pkg ".index.json"))]
                            (.write w (.getBytes (cheshire.core/generate-string index))))))
                      (catch Exception e
                        (println ::re-index-error (str pkg (.getMessage e))))))))))
    :ok))

(defn index-ndjson [context filename]
    (time
     (let [inps (input-stream context DEFAULT_BUCKET filename)
           out-file (str/replace filename #"\.tgz" ".ndjson.gz")]
       (fhir.registry.ndjson/write-stream-ndjson-gz
        (output-stream context DEFAULT_BUCKET out-file)
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

(system/defstart [context cfg]
  (system/set-system-state context [:svc] (mk-service cfg))
  {})

(comment
  (def context (system/start-system {:services ["fhir.registry.gcs"]
                                     :fhir.registry.gcs {:service-account "./sa.json"}}))

  (system/stop-system context)

  (get-bucket context DEFAULT_BUCKET)
  (objects context DEFAULT_BUCKET)

  (time (reduce-package context "hl7.fhir.r4b.core" "4.3.0" (fn [_ file read] (println file))))

  (def pkgs (list-packages context))
  (re-index context)

  (index-ndjson context "-/accdr.fhir.ig.pkg-0.9.21.tgz")

  (list-tgz context)

  (def cnt (atom 0))

  (time
   (->> (lazy-objects context DEFAULT_BUCKET "-/")
        (filter (fn [x] (str/ends-with? (.getName x) ".tgz")))
        (pmap (fn [x]
                (swap! cnt inc)
                (print (str @cnt (.getName x) "\n"))
                (flush)
                (index-ndjson context (.getName x))))
        (doall)))

  


  )
