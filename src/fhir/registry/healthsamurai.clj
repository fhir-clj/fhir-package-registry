(ns fhir.registry.healthsamurai
  (:require [org.httpkit.client]
            [clojure.string :as str]
            [clojure.xml :as xml]
            [clojure.java.io :as io]))

(def FEED_URL "https://storage.googleapis.com/health-samurai-igs/fhir-feed.xml")

(defn parse-title
  "Parse title like 'io.health-samurai.mdm.r5#0.2601.0' into {:name ... :version ...}"
  [title]
  (when title
    (let [[name version] (str/split title #"#" 2)]
      (when (and name version)
        {:name name
         :version version}))))

(defn title->filename
  "Convert title 'name#version' to 'name-version.tgz'"
  [title]
  (when-let [{:keys [name version]} (parse-title title)]
    (str name "-" version ".tgz")))

(defn fetch-feed []
  (let [resp @(org.httpkit.client/get FEED_URL)]
    (when (= 200 (:status resp))
      (xml/parse (java.io.ByteArrayInputStream. (.getBytes (:body resp) "UTF-8"))))))

(defn extract-items
  "Extract items from parsed XML feed"
  [feed]
  (when feed
    (->> feed
         :content
         (filter #(= :channel (:tag %)))
         first
         :content
         (filter #(= :item (:tag %)))
         (mapv (fn [item]
                 (let [content-map (->> (:content item)
                                        (reduce (fn [acc el]
                                                  (assoc acc (:tag el) (first (:content el))))
                                                {}))]
                   {:title (:title content-map)
                    :link (:link content-map)}))))))

(defn packages
  "Returns list of tgz filenames from Health Samurai feed"
  []
  (->> (fetch-feed)
       (extract-items)
       (mapv :title)
       (mapv title->filename)
       (filter identity)))

(defn packages-with-urls
  "Returns [{:filename \"name-version.tgz\" :url \"https://...\"}]"
  []
  (->> (fetch-feed)
       (extract-items)
       (mapv (fn [{:keys [title link]}]
               (when-let [filename (title->filename title)]
                 {:filename filename
                  :url link})))
       (filter identity)))

(comment
  (def feed (fetch-feed))

  (extract-items feed)

  (packages)

  (packages-with-urls)

  (count (packages))

  )
