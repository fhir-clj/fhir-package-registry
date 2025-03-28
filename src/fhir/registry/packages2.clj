(ns fhir.registry.packages2
  (:require [org.httpkit.client]
            [clojure.string :as str]))

(defn packages []
  (->> (str/split (:body @(org.httpkit.client/get "http://packages2.fhir.org/web/")) #"\<a href=\"")
       (mapv (fn [x]
               (let [res (first (str/split x #"\"" 2))]
                 (when (str/ends-with? res ".tgz")
                   res))))
       (filter identity)))

(comment
  (def pkgs (packages))

  (count pkgs)
  4696

  )
