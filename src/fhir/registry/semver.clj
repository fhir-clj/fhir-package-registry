(ns fhir.registry.semver
  (:require [clojure.string :as str]))

(defn parse-semver
  "Parse a semantic version string into a map with :major, :minor, :patch,
   :pre-release, and :build components."
  [version-str]
  (when version-str
    (let [[version-core build] (str/split version-str #"\+" 2)
          [version-nums pre-release] (str/split version-core #"-" 2)
          [major minor patch] (mapv #(Integer/parseInt %)
                                   (str/split version-nums #"\." 3))]
      {:major major
       :minor (or minor 0)
       :patch (or patch 0)
       :pre-release (when pre-release (str/split pre-release #"\."))
       :build (when build build)})))

(defn compare-pre-release
  "Compare pre-release identifiers according to SemVer rules.
   Pre-release versions have lower precedence than the associated normal version."
  [a-pre b-pre]
  (cond
    ;; If both have no pre-release parts, they're equal
    (and (nil? a-pre) (nil? b-pre)) 0
    ;; If a has no pre-release but b does, a is higher (normal > pre-release)
    (nil? a-pre) 1
    ;; If b has no pre-release but a does, b is higher (normal > pre-release)
    (nil? b-pre) -1
    ;; Both have pre-release identifiers, compare them
    :else
    (let [a-parts a-pre
          b-parts b-pre
          max-length (max (count a-parts) (count b-parts))]
      (loop [idx 0]
        (if (= idx max-length)
          ;; If we reached the end of both lists, they're equal
          ;; If one list is shorter, it has lower precedence
          (compare (count a-parts) (count b-parts))
          (let [a-part (get a-parts idx)
                b-part (get b-parts idx)]
            (cond
              ;; If a is shorter, a is lower
              (nil? a-part) -1
              ;; If b is shorter, b is lower
              (nil? b-part) 1
              ;; If both are numeric, compare as integers
              (and (re-matches #"^\d+$" a-part) 
                   (re-matches #"^\d+$" b-part))
              (compare (Integer/parseInt a-part) (Integer/parseInt b-part))
              ;; If only a is numeric, a is lower
              (re-matches #"^\d+$" a-part) -1
              ;; If only b is numeric, b is lower
              (re-matches #"^\d+$" b-part) 1
              ;; Otherwise, compare as strings
              :else (compare a-part b-part))))))))

(defn semver-comparator
  "A comparator function for semantic version strings.
   Returns negative if a < b, positive if a > b, zero if equal."
  [a b]
  (let [a-map (parse-semver a)
        b-map (parse-semver b)]
    (let [major (compare (:major a-map) (:major b-map))]
      (if (= 0 major)
        (let [minor (compare (:minor a-map) (:minor b-map))]
          (if (= 0 minor)
            (let [patch (compare (:patch a-map) (:patch b-map))]
              (if (= 0 patch)
                (compare-pre-release (:pre-release a-map) (:pre-release b-map))
                patch))
            minor))
        major))))

(defn latest [versions]
  (or (->> versions
           (filter (fn [v] (nil? (:pre-release (parse-semver v)))))
           (sort semver-comparator)
           (last))
      (->> versions
           (sort semver-comparator)
           (last))))

(comment
  ;; Sort a list of versions
  (sort semver-comparator 
        ["1.0.0-alpha.1"  "1.0.0" "1.0.0-alpha" "1.0.0-beta" 
         "1.0.0-beta.11" "1.0.0+build.123" "1.1.0" "2.0.0"])

  (latest ["1.0.0-alpha.1"  "1.0.0" "1.0.0-alpha" "1.0.0-beta"
           "1.0.0-beta.11" "1.0.0+build.123" "1.1.0" "2.0.0" "3.0.0-betta"])


  (def vs
    [
     "1.0.0"
     "4.1.0"
     "6.0.0-ballot"
     "5.0.0"
     "5.0.0-ballot"
     "6.0.0"
     "5.0.1"
     "3.1.1"
     "3.0.0"
     "4.0.0"
     "3.1.0"
     "3.2.0"
     "3.0.1"
     "1.0.1"
     "2.0.0"
     "1.1.0"
     "2.1.0"
     "0.0.0"
     ])

  (sort semver-comparator vs)

  (parse-semver "5.0")

  (parse-semver "5.0.0-ballot")

  )
