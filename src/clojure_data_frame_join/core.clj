(ns clojure-data-frame-join.core
  (:require [flambo.sql :as sql]
            [flambo.conf :as conf]
            [flambo.api :as api])
  (:import [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql Column
            RowFactory
            SaveMode])
  (:gen-class))

(defn build-spark-context[app-name]
  (defonce spark-context (api/spark-context (conf/spark-conf)))
  (defonce sql-context
    (sql/sql-context spark-context)))

(defn build-spark-local-context [app-name]
  (defonce spark-context (api/spark-context "local[*]" app-name))
  (defonce sql-context
    (sql/sql-context spark-context)))

(defn build-columns
  "prepare a column array"
  [& mycols]
  (into-array Column (map (fn [x] (Column. x)) mycols)))

(defn str-arry
  "prepare a string array"
  [& mycols]
  (into-array String mycols))

(build-spark-local-context "new-name")

(.show
  (->
   sql-context
   (.read)
   (.format "json")
   (.load "resources/employee.json")))

;; created alaias
(def df1 (->
          (sql/json-file sql-context "resources/employee.json")
          (.select
                   (into-array Column [(.as (Column. "id") "df1id") (.as (Column. "name") "df1name")
                                       (.as (Column. "age") "df1age")]))))

;;
(def df2 (sql/json-file sql-context "resources/employeebonus.json"))

(.show df1)
(.show df2)

;;create a join expression
(.show (.join df1 df2 (.and (.equalTo (Column. "df1id") (Column. "id"))
                            (.equalTo (Column. "df1name") (Column. "name"))) "left_outer"))



