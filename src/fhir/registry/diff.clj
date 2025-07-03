(ns fhir.registry.diff
  (:require [system]
            [pg]
            [http]
            [fhir.registry]))


(comment
  
  (def canonicals 
    (pg.repo/select context {:table :fhir_packages.canonical :where [:in :id [:pg/params-list  ids]]}))

  (def cns
    (pg/execute! 
     context
     {:sql
      "
     create table fhir_packages.elements as
      select 
      package_name, package_version, url, version
      ,element->>'id' as id
      ,element
      from fhir_packages.canonical,
         jsonb_array_elements(resource#>'{differential,element}') as element
      where element is not null
      and package_name in ('hl7.fhir.r4b.core','hl7.fhir.r4.core','hl7.fhir.r5.core', 'hl7.fhir.r6.core')
     "}))
  
  
  (pg/execute! context {:sql "drop table fhir_packages.elements"})
  
  (pg/execute! context {:sql "select count(*) from fhir_packages.elements"})

  (pg/execute! context {:sql "select * from fhir_packages.elements where url ilike '%Patient' limit 10"})

  (pg/execute! context {:sql " create table fhir_packages.element_attrs (
                                 package_name text,
                                 package_version text,
                                 url text,
                                 version text,
                                 id text,
                                 attr text,
                                 value jsonb
                              ) "})

  (pg/execute! context {:sql "insert into fhir_packages.element_attrs 
                              (package_name, package_version, url, version, id, attr, value)
                        select package_name, package_version, url, version, id, kv.key, kv.value
                        from fhir_packages.elements,
                             jsonb_each(element) as kv
                        "})

  (pg/execute! context {:sql "drop table fhir_packages.element_attrs"})

  (pg/execute! context {:sql " create table fhir_packages.element_attrs_ext (
                                 package_name text,
                                 package_version text,
                                 url text,
                                 version text,
                                 id text,
                                 attr text,
                                 value jsonb
                              ) "})

  (pg/execute! context {:sql "drop table fhir_packages.element_attrs_ext"})


  (pg/execute! context {:sql "select * from fhir_packages.element_attrs limit 10"})
  (pg/execute! context {:sql "select attr, count(*) from fhir_packages.element_attrs group by 1 order by 2 desc limit 100"})
  

  (pg/execute! context {:sql "select * from fhir_packages.element_attrs where attr = 'min' limit 10"})

  (pg/execute! context {:sql "insert into fhir_packages.element_attrs_ext 
                              (package_name, package_version, url, version, id, attr, value)
                              select package_name, package_version, url, version, id, attr, value
                              from fhir_packages.element_attrs
                              where attr in ('min', 'max') "})


  (pg/execute! context {:sql "insert into fhir_packages.element_attrs_ext 
                              (package_name, package_version, url, version, id, attr, value)
                              select package_name, package_version, url, version, id, attr || '.type', val->'code'
                              from fhir_packages.element_attrs,
                                   jsonb_array_elements(value) as val
                              where attr in ('type') "})


  (pg/execute! context {:sql "delete from fhir_packages.element_attrs_ext where attr = 'type'"})

  

  (def attrs
    (pg/execute! context {:sql "select 
                              id, attr,
                              array_agg(distinct coalesce(value::text, '~')) as value,
                              array_agg(distinct version) as versions
                              from fhir_packages.element_attrs_ext
   where id ilike 'Encounter.%'
   group by 1,2
   having array_length(array_agg(distinct value::text),1) > 1
   --or array_length(array_agg(distinct version),1) < 9
   "}))

  
  attrs

  (count
   ["4.1.0" "4.3.0" "4.4.0" "4.5.0" "5.0.0" "5.0.0-ballot" "5.0.0-snapshot3" "6.0.0-ballot2" "6.0.0-ballot3"])
  
  attrs

  (def attrs
    (pg/execute! context {:sql "select 
                              id, version, attr, value
                              from fhir_packages.element_attrs_ext
   where id ilike 'Encounter.%'
   order by id, attr, version
   "}))


  attrs
  
  cns
  )


(defn ^{:http {:path "/diff"}}
  diff
  [context request]
  (let []
    (fhir.registry/layout
     context request
     [:div {:class "p-3"}
      (for [a attrs]
      [:div 
       [:b (:id a)]
       [:span {:class "text-gray-300"} "/"]
       [:b (:attr a)]
       (pr-str (:value a))
       (pr-str (:versions a))
       #_(pr-str (dissoc a :id :attr))]
        
        )])))



(def context fhir.registry/context)
(def current-ns *ns*)
(http/register-ns-endpoints context current-ns)