(ns fhir.registry.tar
  (:require [clojure.string :as str]
            [system]
            [clojure.java.io :as io]
            [cheshire.core]
            [fhir.registry.ndjson])
  (:import [java.util Base64]
           [java.net URL]
           [java.io InputStream]
           [org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveInputStream]
           [org.apache.commons.compress.compressors.gzip GzipCompressorInputStream]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStream InputStreamReader BufferedWriter OutputStreamWriter]))

(defn reduce-input [^InputStream input-stream cb & [acc]]
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

(defn find-entry-input [^InputStream input-stream filename]
  (with-open [^GzipCompressorInputStream gzip-compressor-input-stream (GzipCompressorInputStream. input-stream)
              ^TarArchiveInputStream tar-archive-input-stream (TarArchiveInputStream. gzip-compressor-input-stream)]
    (loop []
      (if-let [^TarArchiveEntry entry (.getNextTarEntry tar-archive-input-stream)]
        (if (= filename (str/replace (.getName entry) #"package/" ""))
          (let [content (byte-array (.getSize entry))]
            (.read tar-archive-input-stream content)
            (.close input-stream)
            (String. content))
          (recur))
        nil))))

(defn check-input [^InputStream input-stream]
  (with-open [^GzipCompressorInputStream gzip-compressor-input-stream (GzipCompressorInputStream. input-stream)
              ^TarArchiveInputStream tar-archive-input-stream (TarArchiveInputStream. gzip-compressor-input-stream)]
    (loop [i 0]
      (if-let [^TarArchiveEntry _entry (.getNextTarEntry tar-archive-input-stream)]
        (recur (inc i))
        i))))
