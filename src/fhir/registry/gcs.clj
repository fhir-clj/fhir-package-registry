(ns fhir.registry.gcs
  (:require [clojure.string :as str]
            [system]
            [cheshire.core])
  (:import [com.google.cloud.storage StorageOptions
            BlobInfo BlobId
            Storage Bucket Blob Storage$BucketGetOption
            Blob$BlobSourceOption
            Storage$BlobListOption
            Storage$BlobGetOption
            Blob$BlobSourceOption
            Storage$BlobWriteOption]
           [java.util Base64]
           [java.net URL]
           [java.io InputStream]
           [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
           [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStream InputStreamReader BufferedWriter OutputStreamWriter]))


(set! *warn-on-reflection* false)

(defn gz-stream [^InputStream str]
  (GZIPInputStream. str))


(defn mk-service [cfg]
  (.getService (StorageOptions/getDefaultInstance)))

(defn get-bucket [service bucket-name]
  (.get service ^String bucket-name ^"[Lcom.google.cloud.storage.Storage$BucketGetOption;" (into-array Storage$BucketGetOption [])))

(defn package-file-name [package version file]
  (str "p/" package "/" version "/" file))



(defn objects
  [service ^String bucket & [prefix]]
  (let [bucket (get-bucket service bucket)
        opts (into-array Storage$BlobListOption (if prefix [(Storage$BlobListOption/prefix prefix)] []))
        page   (.list bucket opts)]
    (loop [page page acc (into [] (.getValues page))]
      (if-let [next-page (.getNextPage page)]
        (recur next-page (into acc (.getValues page)))
        (into acc (.getValues page))))))

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
  [service bucket file]
  (let [bid (BlobId/of bucket file)
        blb (.get service bid (into-array Storage$BlobGetOption []))]
    (assert blb (str "FILE NOT EXISTS:" bucket "/" file))
    blb))

(defn input-stream [service bucket file]
  (let [blob (get-blob service bucket file)]
    (Channels/newInputStream (.reader blob (into-array Blob$BlobSourceOption [])))))


(defn output-stream [service bucket ^String file  & [{gz :gzip}]]
  (let [bid (BlobId/of bucket file)
        binfo (BlobInfo/newBuilder bid)
        ch (.writer ^Storage service ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))]
    (Channels/newOutputStream ch)))


;; (defn write-blob [storage bucket file cb]
;;   (with-open
;;     [os (blob-ndjson-writer storage bucket file)
;;      outz (GZIPOutputStream. os)
;;      w (BufferedWriter. (OutputStreamWriter. outz))]
;;     (cb w)))

;; (defn package-file [package version file]
;;   (let [b (get-blob (str "p/" package "/" version "/" file))]
;;     (assert b (str "no file " b))
;;     b))


;; (defn text-blob [storage bucket file content]
;;   (let [bid (BlobId/of bucket file)
;;         binfo (BlobInfo/newBuilder bid)]
;;     (with-open [ch (.writer ^Storage storage ^BlobInfo (.build binfo) (into-array Storage$BlobWriteOption []))
;;                 os (Channels/newOutputStream ch)
;;                 w (BufferedWriter. (OutputStreamWriter. os))]
;;       (.write w content))))

;; (defn write-ndjson-gz [filename cb]
;;   (with-open [writer (-> filename
;;                          (io/output-stream)
;;                          (GZIPOutputStream.)
;;                          (io/writer))]
;;     (cb writer)))


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




(comment
  (def svc (mk-service {}))

  (get-bucket svc DEFAULT_BUCKET)

  (with-open [w (output-stream svc DEFAULT_BUCKET "/-/test")]
    (.write w (.getBytes "Hello")))

  (with-open [i (input-stream svc DEFAULT_BUCKET "/-/test")]
    (slurp i))

  (str/replace "fiel.tgz" #"(\.tgz|\.json)$" "")

  (def idx
    (->> (objects svc DEFAULT_BUCKET "-") (sort-by #(.getName %))
         (reduce (fn [acc o]
                   (let [file-name (last (str/split (.getName o) #"/"))
                         _ (println file-name)
                         ext (last (str/split file-name #"\."))
                         pkg-name (str/replace file-name #"(\.tgz|\.json)$" "")]
                     (if (and (contains? #{"json" "tgz"} ext) (not (str/ends-with? file-name ".index.json")))
                       (assoc-in acc [pkg-name ext] o)
                       acc)))
                 {})))

  (time
   (->> idx
        (mapv (fn [[pkg {json "json" tgz "tgz"}]]
                (println pkg)
                (when (and tgz (not json))
                  (let [res (time (reduce-tar (blob-input-stream tgz) (fn [acc file read]
                                                                      (cond (= "package.json" file)
                                                                            (assoc acc :package (read true))
                                                                            (= ".index.json" file)
                                                                            (assoc acc :index (read true))
                                                                            :else acc))))]
                    (when-let [package (:package res)]
                      (println (str "-/" pkg ".json"))
                      (with-open [w (output-stream svc DEFAULT_BUCKET (str "-/" pkg ".json"))]
                        (.write w (.getBytes (cheshire.core/generate-string package)))))
                    (when-let [index (:index res)]
                      (println (str "-/" pkg ".index.json"))
                      (with-open [w (output-stream svc DEFAULT_BUCKET (str "-/" pkg ".index.json"))]
                        (.write w (.getBytes (cheshire.core/generate-string index)))))))))))

  (def files (objects svc DEFAULT_BUCKET "-"))

  (def f (second files))
  (def f (nth files 4))

  (def s (.getContent f (into-array Blob$BlobSourceOption [])))

  (def res2
    )

  res

  res2

  (.getSize f)

  (bean f)
  (type f)

  (.getName (nth files 2))

  (bean (second files))



  )
