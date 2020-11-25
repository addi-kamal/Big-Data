-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Spark SQL

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Les commandes suivantes crée une table à partir d'un ensemble de données Databricks

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds
USING csv
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Afficher la totalité de la table créer :

-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").save("/delta/diamonds")

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## La requête suivante manipule les données et affiche les résultats :
-- MAGIC 
-- MAGIC Plus précisément, la commande:
-- MAGIC 1. Sélectionne les colonnes "color" et "price" et la moyenne du prix et regroupe les résultats par couleur et les afficher par ordre.
-- MAGIC 1. Affiche un tableau des résultats.

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Repeat the same operations using Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter. To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command creates a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC 
-- MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))
