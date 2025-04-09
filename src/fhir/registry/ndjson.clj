(ns fhir.registry.ndjson
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core])
  (:import [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.channels Channels]
           [java.io BufferedReader InputStreamReader InputStream BufferedWriter OutputStreamWriter BufferedReader IOException]
           [java.net MalformedURLException URL URLConnection]
           [java.io BufferedReader InputStreamReader BufferedWriter OutputStreamWriter]))

(defn write-ndjson-gz [filename cb]
  (with-open [writer (-> filename (io/output-stream) (GZIPOutputStream.) (io/writer))]
    (let [write (fn [^String s] (.write writer s) (.write writer "\n"))]
      (cb write))))

(defn write-stream-ndjson-gz [outputstream cb]
  (with-open [writer (-> outputstream (GZIPOutputStream.) (io/writer))]
    (let [write (fn [^String s] (.write writer s) (.write writer "\n"))]
      (cb write))
    (.flush writer)))

(defn read-stream
  "process-fn (fn [json line-number]), if no read into vector"
  [^InputStream input-stream & [process-fn acc]]
  (with-open [gz-stream (GZIPInputStream. input-stream)
              reader (-> gz-stream InputStreamReader. BufferedReader.)]
    (loop [line (.readLine reader)
           line-number 0
           acc (or acc [])]
      (if line
        (let [res (cheshire.core/parse-string line keyword)
              acc (if process-fn (process-fn acc res line-number) (conj acc res))]
          (recur (.readLine reader) (inc line-number) acc))
        acc))))

(defn process-stream
  "process-fn (fn [json line-number])"
  [^InputStream input-stream process-fn]
  (with-open [gz-stream (GZIPInputStream. input-stream)
              reader (-> gz-stream InputStreamReader. BufferedReader.)]
    (loop [line (.readLine reader)
           line-number 0]
      (if line
        (let [res (cheshire.core/parse-string line keyword)]
          (process-fn res line-number)
          (recur (.readLine reader) (inc line-number)))))))

(defn url-stream [^String url]
  (let [^URL url (URL. url)
        ^URLConnection conn  (.openConnection url)]
    (.getInputStream conn)))


