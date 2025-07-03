(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.java.shell :refer [sh]]))

(def class-dir "target/classes")
(def uber-file "target/fhir-registry.jar")

;; delay to defer side effects (artifact downloads)
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (sh "tailwindcss" "-i" "./resources/public/app.css" "-o" "./resources/public/app.build.css")
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[fhir.registry]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @basis
           :main 'fhir.registry
           :exclude [#"META-INF/license/.*"]}))