# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL et Dataframes :

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Les commandes suivantes crée une table à partir d'un ensemble de données Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC 
# MAGIC CREATE TABLE diamonds
# MAGIC USING csv
# MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Afficher la totalité de la table créer :

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from diamonds
# MAGIC LIMIT 10  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diamonds WHERE clarity LIKE 'V%' AND price <2000
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT carat, clarity, price FROM diamonds ORDER BY price DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## La requête suivante manipule les données et affiche les résultats :
# MAGIC 
# MAGIC Plus précisément, la requête:
# MAGIC 1. Sélectionne les colonnes "color" et "price" et la moyenne du prix et regroupe les résultats par couleur et avec l'ordre alphabetique.
# MAGIC 1. Affiche un tableau des résultats.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color

# COMMAND ----------

# MAGIC %md 
# MAGIC ## On répéte les mêmes opérations à l'aide de l'API DataFrame de Python.
# MAGIC Ceci est un notebook SQL, par défaut, les instructions de commande sont transmises à un interpréteur SQL. Pour passer des instructions de commande à un interpréteur Python, on utilise la commande "% python".

# COMMAND ----------

# MAGIC %md
# MAGIC ## La commande suivante créer un DataFrame à partir d'un ensemble de données Databricks :

# COMMAND ----------

# MAGIC %python
# MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Afficher les 5 premiers lignes de notre Dataframe :

# COMMAND ----------

# MAGIC %python
# MAGIC display(diamonds.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## La table des "color", moyennes "price" qu'on a déjà fait avec SQL :

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import avg
# MAGIC 
# MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))
