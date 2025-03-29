(ns fhir.registry
  (:require
   [system]
   [http]
   [pg]
   [pg.repo]
   [pg.docker :as pgd]
   [cheshire.core]
   [uui]
   [uui.heroicons :as ico]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.set]
   [ring.middleware.multipart-params :as multipart]
   [clojure.walk]
   [fhir.registry.gcs :as gcs]
   [fhir.registry.legacy]
   [org.httpkit.client]
   [fhir.registry.semver :as semver]
   [fhir.registry.ndjson :as ndjson]
   [fhir.registry.index]
   [fhir.registry.database])
  (:import
   [java.util Base64]))

(system/defmanifest
  {:description "FHIR Registry"
   :deps ["pg" "pg.repo" "http"]
   :config {:history {:type "boolean"}}})

(def current-ns *ns*)

(defn html? [request]
  (str/includes? (get-in request [:headers "accept"] "") "http"))

(defn json? [request]
  (or (str/includes? (get-in request [:headers "accept"] "") "json")
      (= "json" (get-in request [:query-params :_format]))))

(defn layout [context request content]
  (uui/boost-response
   context request
   [:div
    [:div {:class "px-10 flex items-center space-x-4 border-b border-gray-300"}
     [:b "FHIR Packages"]
     [:a {:href "/"   :class "text-sm px-2 py-3"} "Packages"]
     [:a {:href "/canonicals" :class "text-sm px-2 py-3"} "Canonicals"]
     [:a {:href "/timeline"   :class "text-sm px-2 py-3"} "Logs"]
     [:a {:href "/problems"   :class "text-sm px-2 py-3"} "Problems"]
     [:div {:class "flex-1"}]
     ]
    [:div {:class "py-3 px-6"} content]]))

(defn elipse [txt & [limit]]
  (when txt
    [:span (subs txt 0 (min (count txt) (or limit 100))) "..."]))


(defn search-input [request {url :url ph :placeholder trg :target dont-push :dont-push}]
  (let [q (:search (:query-params request))]
    [:div {:class "grid grid-cols-1"}
     [:input
      {:class "border border-gray-300 col-start-1 row-start-1 block w-full rounded-xl py-1.5 pr-3 pl-10 text-base text-gray-900 outline-1 -outline-offset-1 outline-gray-300 placeholder:text-gray-400 focus:outline-2 focus:-outline-offset-2 focus:outline-indigo-600 sm:pl-9 sm:text-sm/6"
       :type "search"
       :name "search",
       :placeholder (or ph "Search..")
       :autofocus true
       :hx-get url
       :hx-trigger "input changed delay:300ms, search",
       :hx-target (or trg "#search-results",)
       :hx-push-url (when-not dont-push "true")
       :hx-indicator ".htmx-indicator"
       :value q}]
     (ico/magnifying-glass "size-5 pointer-events-none col-start-1 row-start-1 ml-3 size-5 self-center text-gray-400 sm:size-4")]))

(defn search-packages [context request]
  (let [q (:search (:query-params request))]
    (pg/execute! context {:dsql {:select [:pg/list
                                          :name
                                          [:pg/sql "array_agg(distinct author) as authors"]
                                          [:pg/sql "array_agg(version) as versions"]
                                          [:pg/sql "array_agg(distinct \"fhirVersions\"[1]) as \"fhirVersions\""]
                                          ]
                                 :from :fhir_packages.package
                                 :where (when (and q (not (str/blank? q)))
                                          [:ilike :name [:pg/param (str "%" (str/replace q #"\s+" "%") "%")]])
                                 :group-by 1
                                 :limit 100}})))

(defn packages-grid [packages]
  [:div#search-results {:class "mt-4 divide-y divide-gray-200"}
   [:table.uui {:class "text-sm"}
    [:thead
     [:tr [:th "package"] [:th "latest"]  [:th "author"] [:th "FHIR"] [:th "Versions"]]]
    [:tbody
     (for [pkg packages]
       [:tr {:class ""}
        [:td
         (ico/cube "size-4 text-orange-500/50 inline mr-2")
         [:a {:class "py-2 text-sky-600" :href (str "/" (:name pkg))} (:name pkg)]]
        [:td (last (sort semver/semver-comparator (:versions pkg)))]
        [:td {:class "text-sm text-gray-500"} (first (:authors pkg))]
        [:td {:class "text-xs text-gray-500"} (str/join ", " (filter identity (:fhirVersions pkg)))]
        [:td {:class "text-xs text-gray-500"} (count (:versions pkg))]
        ])]]])

(defn packages-view [context request packages]
  (if (uui/hx-target request)
    (uui/response (packages-grid packages))
    (layout
     context request
     [:div {:class "p-3"}
      (search-input request {:url "/" :placeholder "Search with prefix: hl7 fhir r5"})
      (packages-grid packages)])))

(defn ^{:http {:path "/"}}
  index
  [context request]
  (let [packages (search-packages context request)]
    (if (not (json? request))
      (packages-view context request packages)
      {:body packages
       :headers {"content-type" "application/json"}
       :status 200})))


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

(defn get-package [context package-name]
  (let [versions (->> (pg.repo/select context {:table "fhir_packages.package" :match {:name package-name}})
                      (reduce (fn [acc {v :version :as package}]
                                (assoc acc v (format-package package)))
                              {}))]
    (build-package-json versions)))


(comment

  (get-package context "hl7.fhir.us.core")
  (get-package context "hl7.fhir.r4.core")

  )

(defn copy-block [cmd & [title]]
  (let [id (str "code-" (gensym))
        marker-id (str id "-marker")]
    [:div {:class "mt-4 border border-gray-300 bg-gray-100 rounded-md  text-xs"}
     (when title [:div {:class "text-xs border-b border-gray-300 text-center text-gray-500 bg-gray-200"} title])
     [:div {:class "flex items-center"}
      [:code {:id id :class "flex-1 font-mono py-1 px-4"} cmd]
      [:a {:class "px-1 py-1 cursor-pointer hover:text-sky-600 rounded-sm"
           :onclick (format "copyCode('%s','%s')" id marker-id)}
       [:div {:class "relative"}
        (ico/clipboard "size-6" :outline)
        [:div {:id marker-id :class "hidden absolute top-0 left-0"} (ico/check "text-green-600 size-6")]]]]]))

(defn ^{:http {:path "/:package"}}
  package
  [context {{package-name :package} :route-params :as request}]
  (let [package  (get-package context package-name)
        latest-v (get-in package [:dist-tags :latest])]
    (if-not package
      {:status 404}
      (if (not (json? request))
        (layout
         context request
         [:div {:class "p-3"}
          (uui/breadcramp
           ["/" "Packages"]
           ["#" (:name package)])
          [:div {:class "flex items-top space-x-8"}
           [:div {:class "py-3 w-3xl" }
            [:h1.uui {:class "border-b py-2"} (:name package)]
            (copy-block (str "npm install --registry http://fs.get-ig.org/pkgs " (:name package)) "Install package")
            (copy-block (format "curl http://fs.get-ig.org/rs/%s | gunzip | jq '{url,resourceType,version, id}'"
                                (str (:name package) "-" latest-v ".ndjson.gz"))
                        "Get resources")
            [:p {:class "mt-4 text-gray-600 text-sm w-3xl"}
             (:description package)]
            [:details
             [:summary "package.json"]
             (uui/json-block package)]]
           [:div
            [:h2.uui "Versions"]
            [:div {:class "divide-y divide-gray-300"}
             (for [v (:_versions package)]
               (let [pkg (get-in package [:versions v])]
                 [:div {:class "py-1"}
                  [:a {:class "text-sky-600 flex items-center space-x-2" :href (str "/" (:name pkg) "/" (:version pkg))}
                   (if (= latest-v v)
                     (ico/star "size-4 text-red-400")
                     (ico/star "size-4 text-gray-400" :outline))
                   [:span (:version pkg)]
                   ]]))]]]])
        {:body package :status 200}))))

;; TODO: uui rpc guard with meta on function
;; TODO: dsql escape :cammelCase
;; TODO: bug with - in package name :(
(defn package-canonicals [context request opts]
  (let [canonicals (->> (pg/execute! context {:dsql {:select [:pg/list :id :url :version :_filename [:pg/sql "\"resourceType\""]]
                                                    :from :fhir_packages.canonical
                                                    :where [:and
                                                            [:= :package_name (:name opts)]
                                                            [:= :package_version (:version opts)]]
                                                    :limit 1000}})
                       (sort-by :_filename))]
    [:div {:class "mt-4"}
     [:table.uui {:class "text-sm"}
      [:thead [:tr [:th "resourceType"] [:th "file"]]]
      [:tbody
       (for [c canonicals]
         [:tr
          [:td (:resourceType c)]
          [:td [:a {:title (str (:url c) "|" (:version c))
                    :href (str "/canonicals/" (:id c)) :class "text-sky-600"} (:_filename c)] ]])]]]))

(defn ^{:http {:path "/:package/:version"}}
  package-version
  [context {{package :package version :version} :route-params :as request}]
  (if-let [package   (pg.repo/read context {:table "fhir_packages.package" :match {:name package :version version}})]
    (let [package (format-package package)
          deps      (pg/execute! context {:sql ["select * from fhir_packages.package_dependency where source_name = ? and source_version =?"
                                                (:name package) (:version package)]})
          dependant (pg/execute! context {:sql ["select * from fhir_packages.package_dependency where destination_name = ? and destination_version =? order by source_name, source_version"
                                                (:name package) (:version package)]})]
      (if (not (json? request))
        (layout
         context request
         [:div {:class "p-3" }
          (uui/breadcramp ["/" "Packages"] [(str "/" (:name package)) (:name package)] ["#" (:version package)])
          [:div {:class "flex space-x-8"}
           [:div {:class "w-3xl" }
            [:h1.uui {:class  "flex items-center space-x-8 border-b py-2"}
             [:span {:class "flex-1"} (:name package)  "@" (:version package)]
             (map (fn [x] [:span {:class "flex items-center"} (ico/fire "size-4") [:span x]]) (:fhirVersions package))
             [:a {:href (str "http://fs.get-ig.org/-/" (:name package) "-" (:version package) ".tgz")
                  :class "border px-2 py-1 hover:bg-gray-100 rounded border-gray-300 hover:text-sky-600 text-gray-600"}
              (ico/cloud-arrow-down "size-4")]]
            (copy-block (str "npm install --registry http://fs.get-ig.org/pkgs " (:name package) "@" (:version package)) "Install package")
            (copy-block (format "curl http://fs.get-ig.org/rs/%s | gunzip | jq '{url,resourceType,version, id}'"
                                (str (:name package) "-" (:version package) ".ndjson.gz"))
                        "Get resources")
            [:p {:class "mt-4 text-gray-600 text-sm"}
             (:description package)]

            [:details {:class "mt-4"}
             [:summary "package.json"]
             (uui/json-block package)]

            [:div
             {:class "mt-4"
              :hx-get (uui/rpc #'package-canonicals (select-keys package [:name :version]))
              :hx-trigger "load"
              :hx-swap "outerHTML"}
             [:div {:class "text-gray-400 py-2"} "Loading content..."]]]

           [:div
            (when (seq deps)
              (list
               [:h2.uui "Deps"]
               [:div {:class "divide-y divide-gray-200 text-sm"}
                (for [d deps]
                  [:div {:class "py-1  flex items-center space-x-4"}
                   (ico/arrow-right "size-4 text-gray-400")
                   [:a {:class "text-sky-600" :href (str "/" (:destination_name d) "/" (:destination_version d))}
                    [:span (:destination_name d)] "@" [:span (:destination_version d)]]])]))
            (when (seq dependant)
              (list
               [:h2.uui "Dependents"]
               [:div {:class "divide-y divide-gray-200 text-sm"}
                (for [d dependant]
                  [:div {:class "py-1 flex items-center space-x-4"}
                   (ico/arrow-left "size-4 text-gray-400")
                   [:a {:class "text-sky-600" :href (str "/" (:source_name d) "/" (:source_version d))}
                    [:span (:source_name d)] "@" [:span (:source_version d)]]])]))
            ]]])
        {:body package :status 200}))
    (if (not (json? request))
      (layout context request [:div {:class "px-6 text-red-600"} (str package "#" version " not found")])
      {:status 404})))

(defn get-broken-deps [context]
  (pg/execute! context {:sql "
select d.* from fhir_packages.package_dependency d
left join fhir_packages.package p on p.name = d.destination_name and p.version = d.destination_version
where p.id is null
order by d.source_name, d.source_version
limit 1000
"}))

(defn search-canonicals [context request]
  (let [q (:search (:query-params request))]
    #_(println
     (str/join "\n"
               (pg/execute! context {:dsql {:explain {:analyze true}
                                            :select [:pg/list
                                                     :id :url :version :package_name :package_version :_filename
                                                     [:pg/sql "resource->>'resourceType' as resourcetype"]]
                                            :from :fhir_packages.canonical
                                            ;; :order-by [:pg/list :url :version]
                                            :where (when (and q (not (str/blank? q)))
                                                     [:ilike [:pg/sql "url || '|' || coalesce(version,package_version)"]
                                                      [:pg/param (str "%" (str/replace q #"\s+" "%") "%")]])
                                            :limit 300}})))
    (pg/execute! context {:dsql {:select [:pg/list :id :url :version :package_name :package_version :_filename [:pg/sql "\"resourceType\""]]
                                 :from :fhir_packages.canonical
                                 ;; :order-by [:pg/list :url :version]
                                 :where (when (and q (not (str/blank? q)))
                                          [:ilike [:pg/sql "url || '|' || coalesce(version,package_version)"]
                                           [:pg/param (str "%" (str/replace q #"\s+" "%") "%")]])
                                 :limit 300}})))

(defn canonicals-grid [context request canonicals]
  [:div#search-results {:class "mt-4"}
   [:table.uui {:class "text-sm"}
    [:tbody
     (for [can canonicals]
       [:tr
        [:td (:resourceType can)]
        [:td [:a {:href (str "/canonicals/" (:id can)) :class "text-sky-700"}
              (:url can) "|" (or (:version can) (:package_version can))]]
        [:td (:type can)]
        [:td [:a {:class "text-sky-700"
                  :href (str "/" (:package_name can) "/" (:package_version can))}
              (:package_name can)  "@" (:package_version can)]]
        ])]]])

(defn ^{:http {:path "/canonicals"}}
  canonicals
  [context request]
  (let [canonicals (search-canonicals context request)]
    (if (uui/hx-target request)
      (uui/response (canonicals-grid context request canonicals))
      (layout
       context request
       [:div {:class "p-3" }
        (search-input request {:url "/canonicals" :placeholder "Search with prefix: hl7 fhir r5"})
        (canonicals-grid context request canonicals)]))))

(defn ^{:http {:path "/canonicals/:id"}}
  show-canonical
  [context {{id :id} :route-params :as request}]
  (let [canonical (pg.repo/read context {:table :fhir_packages.canonical :match {:id id}})]
    (layout
     context request
     [:div {:class "p-3" }
      (uui/breadcramp
       ["/" "Packages"]
       [(str "/" (:package_name canonical) "/" (:package_version canonical))
        (str (:package_name canonical) "@" (:package_version canonical))]
       ["#" (:url canonical)])
      [:div {:class "mt-4"}
       (uui/json-block canonical)]])))


(defn ^{:http {:path "/timeline"}}
  timeline
  [context request]
  (let [broken-deps (get-broken-deps context)]
    (layout
     context request
     [:div {:class "p-3" }
      [:h1.uui "TBD"]])))

(defn ^{:http {:path "/problems"}}
  problems
  [context request]
  (let [broken-deps (get-broken-deps context)]
    (layout
     context request
     [:div {:class "p-3" }
      [:h1.uui "Broken package deps"]
      [:table.uui {:class "mt-4 text-xs"}
       [:tbody
        (for [dep broken-deps]
          [:tr {:class "border-b border-gray-200"}
           [:td [:a {:class "text-sky-600"
                     :href (str "/" (:source_name dep) "/" (:source_version dep))}
                 (:source_name dep) "@" (:source_version dep)]]
           [:td {:class "py-1 text-gray-600"} "->"]
           [:td {:class "py-1 text-red-600"} (:destination_name dep) "@" (:destination_version dep)]])]]])))

(defn decode-base64-to-file
  "Decodes a base64 string and writes it to a file.
   Returns {:success true, :file path} on success or
   {:success false, :error message} on failure."
  [base64-string output-file-path]
  (try
    (let [decoder (Base64/getDecoder)
          decoded-bytes (.decode decoder base64-string)]
      (with-open [out (io/output-stream output-file-path)]
        (.write out decoded-bytes))
      {:success true, :file output-file-path})
    (catch Exception e
      (println (.getMessage e)))))

;; https://github.com/HealthIntersections/fhirserver/blob/master/server/package_spider.pas
;; https://fhir.github.io/ig-registry/package-feeds.json
;; https://github.com/HealthIntersections/fhirserver/blob/master/server/package_spider.pas

(defn ^{:http {:path "/:package" :method :put}}
  publish-package
  [context {{pkg :package} :route-params :as request}]
  (let [package (cheshire.core/parse-stream (io/reader (:body request)) keyword)
        package-file (str "packages/" (:name package) ".json")
        prev-package (when (.exists (io/file package-file))
                       (cheshire.core/parse-string (slurp package-file) keyword))
        package-resource (update (dissoc package :_attachments) :versions merge (:versions prev-package))]
    (spit package-file (cheshire.core/generate-string package-resource {:pretty true}))
    (pg.repo/upsert context {:table "fhir_packages.package" :resource package-resource})
    (let [package-version (first (vals (:versions package)))
          package-version (assoc package-version :id (str (:name package-version) "@" (:version package-version)))]
      (pg.repo/upsert context {:table "fhir_packages.package_version" :resource package-version})
      (doseq [[dep-name v] (:dependencies package-version)]
        (pg.repo/upsert context {:table "fhir_packages.package_dependency"
                                 :resource {:id (java.util.UUID/randomUUID)
                                            :source_id (:id package-version)
                                            :source_name (:name package-version)
                                            :source_version (:version package-version)
                                            :destination_name (name dep-name)
                                            :destination_version v}})))
    (doseq [[k v] (:_attachments package)]
      (let [out-file (str "packages/" (:name package) "/" (name k))]
        (io/make-parents out-file)
        (decode-base64-to-file (:data v) out-file))))
  {:status 200})


;; https://marmelab.com/blog/2022/12/22/how-to-implement-web-login-in-a-private-npm-registry.html
(defn ^{:http {:path "/-/v1/login" :method :post}}
  login
  [context request]
  (println :login (slurp (:body request)))
  {:status 200
   :body {:loginUrl "http://localhost:3333/login"
          :doneUrl  "http://localhost:3333/login/done"}})

(defn ^{:http {:path "/login/done" :method :get}}
  login-done
  [context request]
  {:status 200
   :body {:token "token"}})

(defn ^{:http {:path "/-/user/:login" :method :put}}
  adduser
  [context request]
  {:status 200
   :body {}})


(defn format-relative-time
  [^java.time.Instant instant]
  (let [now (java.time.Instant/now)
        duration (java.time.Duration/between instant now)
        minutes-ago (.toMinutes duration)
        hours-ago (.toHours duration)
        days-ago (.toDays duration)
        formatter (java.time.format.DateTimeFormatter/ofPattern "MMM d, yyyy")]
    (cond
      (< minutes-ago 1) "just now"
      (< minutes-ago 60) (format "%d %s ago" minutes-ago (if (= 1 minutes-ago) "minute" "minutes"))
      (< hours-ago 24) (format "%d %s ago" hours-ago (if (= 1 hours-ago) "hour" "hours"))
      (< days-ago 3) (format "%d %s ago" days-ago (if (= 1 days-ago) "day" "days"))
      :else (.format formatter instant))))

(defn ^{:http {:path "/timeline" :method :put}}
  timeline
  [context request]
  (let [packages (pg/execute! context {:sql "select * from fhir_packages.import order by lsn desc limit 200"})]
    (layout
     context request
     [:div {:class "p-3"}
      [:h1.uui "Timeline"]
      [:table.uui

       [:tbody 
        (for [pkg packages]
          [:tr
           [:td (:lsn pkg)]
           [:td [:a {:class "text-sky-600" :href (str "/" (:name pkg) "/" (:version pkg))}
                 (:name pkg) "@" (:version pkg)]]
           [:td (if (:resources_loaded pkg)
                  (ico/check-circle "size-4")
                  "-")]
           [:td (format-relative-time (.toInstant (:created_at pkg)))]])]]])))


(system/defstart
  [context config]
  (pg/migrate-prepare context)
  (pg/migrate-up context)
  (http/register-ns-endpoints context current-ns)
  {})

(def default-config
  {:services ["pg" "pg.repo" "http" "uui" "fhir.registry" "fhir.registry.gcs"
              "fhir.registry.index"
              "fhir.registry.database"]
   :http {:port 3333
          :max-body 108388608}})


;; TODO: add envs to system
(defn main [& args]
  (def pg-config {:database "registry" :user "registry" :port 5432 :host "localhost" :password (System/getenv "PG_PASSWORD")})
  (def context (system/start-system (assoc default-config
                                           :pg pg-config
                                           :fhir.registry.gcs {:service-account "./sa.json"}))))


(defn start-dev []
  (def pg-config (pgd/ensure-pg "fhir-registry"))
  (def context (system/start-system (assoc default-config :pg pg-config :fhir.registry.gcs {:service-account "./sa.json"}))))

(defn stop-dev []
  (system/stop-system context))


(comment
  (require '[system.dev :as dev])
  (dev/update-libs)

  ;; (def context (system/start-system {:pg pg-config :services ["pg"]}))

  ;; (pgd/delete-pg "fhir-registry")

  (start-dev)
  (stop-dev)

  (pg/execute! context {:sql "select count(*) from fhir_packages.canonical"})

  (start-dev)

  ;; (pg/generate-migration "fhir_packages_logs")

  (pg/migrate-up context "fhir_packages_logs")
  (pg/migrate-down context "fhir_packages_logs")



  )
