(ns fhir.registry.database
  (:require [system]
            [pg]
            [pg.repo]
            [clojure.string :as str]
            [fhir.registry.ndjson]
            [fhir.registry.gcs :as gcs]))


#_(defn load-canonicals [context]
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


(defn sync-packages [context]
  (let [last-lsn (or (:lsn (first (pg/execute! context {:sql "select * from fhir_packages.import order by lsn desc limit 1"}))) -1)]
    (->> (gcs/read-feed context)
         (filter (fn [x] (> (:lsn x) last-lsn)))
         (pmap (fn [pkg]
                 (println :load pkg)
                 (pg.repo/upsert context {:table :fhir_packages.import  :resource (assoc pkg :id (str (:name pkg) "@" (:version pkg) "-" (:lsn pkg)))})
                 (pg.repo/upsert context {:table :fhir_packages.package :resource (assoc pkg :id (str (:name pkg) "@" (:version pkg)))})
                 (pg/execute! context {:sql ["delete from fhir_packages.package_dependency where source_name = ? and source_version = ?" (:name pkg) (:version pkg)]})
                 (doseq [[dep-name v] (:dependencies pkg)]
                   (pg.repo/upsert context {:table "fhir_packages.package_dependency"
                                            :resource {:id (str (:name pkg) "@" (:version pkg) "-" (name dep-name) (:destination_name pkg) "@" v)
                                                       :source_id (str (:name pkg) "@" (:version pkg))
                                                       :source_name (:name pkg)
                                                       :source_version (:version pkg)
                                                       :destination_name (name dep-name)
                                                       :destination_version v}})))))))

(defn load-package-canonicals [context {package-name :name package-version :version :as package}]
  ;; (println file-name)
  (let [file-name (str "rs/" package-name "-" package-version ".ndjson.gz")]
    (pg/execute! context {:sql ["delete from fhir_packages.canonical where package_name = ? and package_version = ?" package-name package-version]})
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

(defn sync-canonicals [context]
  (->> (pg/execute! context {:sql "select * from fhir_packages.import where resources_loaded = false limit 100"})
       (pmap (fn [pkg]
               (try
                 (load-package-canonicals context pkg)
                 (println :> (pg.repo/upsert context {:table :fhir_packages.import :resource (assoc pkg :resources_loaded true)}))
                 (catch Exception e
                   (println ::resources-sync-error (.getMessage e))))))))

(def missed-canonicals-sql
  "
select name, version
from fhir_packages.package p
where not exists (select 1 from fhir_packages.canonical c where c.package_name = p.name and c.package_version = p.version limit 1)
limit ?
")

(defn sync-missed-canonicals [context limit]
  (let [missed-pkgs (pg/execute! context {:sql [missed-canonicals-sql limit]})]
    (->> missed-pkgs
         (pmap (fn [pkg]
                 (try
                   (load-package-canonicals context pkg)
                   (println :> (:name pkg) (:version pkg))
                   (catch Exception e
                     (println ::resources-sync-error (.getMessage e)))))))))

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

(system/defmanifest
  {:description "FHIR Registry Database Sync"
   :deps ["pg" "pg.repo"]
   :config {}})

(system/defstart
  [context config]
  (println ::start)
  (start-periodic-job "packages-sync" (* 5 60 1000) (fn [] (sync-packages context)))
  (start-periodic-job "canonicals-sync" (* 5 60 1000) (fn [] (sync-canonicals context)))
  {:jobs ["packages-sync" "canonicals-sync"]})

(system/defstop
  [context state]
  (doseq [j (:jobs state)]
    (println :stop j) (stop-periodic-job j)))


(comment
  (def context fhir.registry/context)

  (->> (gcs/read-feed context)
       (take 1)
       #_(mapv (fn [x] (select-keys x [:name :version :lsn]))))


  (pg/execute! context {:sql "truncate fhir_packages.import"})

  (def pkg (first (gcs/read-feed context)))

  pkg

  (pg.repo/clear-table-definitions-cache context)

  (pg/execute! context {:sql "select * from fhir_packages.import order by lsn desc limit 10"})


  (pg/execute! context {:sql "select * from fhir_packages.package limit 10"})

  (pg/migrate-up context "fhir_packages_logs")
  (pg/migrate-down context "fhir_packages_logs")

  (sync-packages context)
  (sync-canonicals context)

  (sync-missed-canonicals context 10)

  )
