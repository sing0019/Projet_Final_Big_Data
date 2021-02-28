# -*- coding: utf-8 -*-

#Importation
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import configparser

#Instanciation
spark = SparkSession.builder \
                    .master("local") \
                    .appName("Projet_Final") \
                    .getOrCreate()

"""## Créer les données."""

# La table Auteur
T1 = [('07890', 'Jean Paul Sartre'),
      ('05678', 'Pierre de Ronsard')]
rdd1 = spark.sparkContext.parallelize(T1)
Author = rdd1.toDF(['aid', 'name'])
Author.createOrReplaceTempView('AuthorSQL')
Author.show(truncate = False)

# # La table book
T2 = [('0001', "L'existentialisme est un humanisme", 'Philosophie'),
      ('0002', "Huis clos. Suivi de Les Mouche", 'Philosophie'),
      ('0003', "Mignonne allons voir si la rose", 'poeme'), 
      ('0004', "Les Amours", 'poeme')]
rdd2 = spark.sparkContext.parallelize(T2)
Book = rdd2.toDF(['bid', 'title', 'category'])
Book.createOrReplaceTempView('BookSQL')
Book.show(truncate = False)

# # La table Student
T3 = [('S15', 'toto', 'Math'), 
      ('S16','popo', 'Eco'),
      ('S17','fofo', 'Mécanique')]
rdd3 = spark.sparkContext.parallelize(T3)
Student = rdd3.toDF(['sid', 'sname', 'dept'])
Student.createOrReplaceTempView('StudentSQL')
Student.show(truncate = False)

# La table write
T4 = [('07890', '0001'), 
      ('07890', '0002'),
      ('05678', '0003'),
      ('05678', '0003')]
rdd4 = spark.sparkContext.parallelize(T4)
Write = rdd4.toDF(['aid', 'bid'])
Write.createOrReplaceTempView('writeSQL')
Write.show(truncate = False)

# La table borrow 
T5 = [('S15', '0003', '02-01-2020', '01-02-2020'), 
      ('S15', '0002', '13-06-2020', 'null'), 
      ('S15', '0001', '13-06-2020', '13-10-2020'),
      ('S16', '0002', '24-01-2020', '24-01-2020'), 
      ('S17', '0001', '12-04-2020', '01-07-2020')]
rdd5 = spark.sparkContext.parallelize(T5)
Borrow = rdd5.toDF(['sid', 'bid', 'checkout_time', 'return_time'])
Borrow.createOrReplaceTempView('borrowSQL')
Borrow.show(truncate = False)

"""### 1-Trouver les titres de tous les livres que l'étudiant sid='S15' a emprunté."""

### DSL
Book.join(Borrow, ["bid"]) \
    .select("sid", "title") \
    .filter(F.col("sid") == "S15") \
    .show(truncate = False)

# En SQL
spark.sql('''select sid, title 
                from BookSQL 
                join borrowSQL
                on BookSQL.bid = borrowSQL.bid
                where borrowSQL.sid = 'S15' ''').show(truncate = False)

"""### 2-Trouver les titres de tous les livres qui n'ont jamais été empruntés par un étudiant."""

### DSL
Book.join(Borrow, ["bid"], how="left_anti") \
    .select("*") \
    .show(truncate = False)

# En SQL
spark.sql( """SELECT * FROM BookSQL
                 LEFT ANTI JOIN borrowSQL
                 ON borrowSQL.bid = BookSQL.bid """).show(truncate = False)

"""### 3-Trouver tous les étudiants qui ont emprunté le livre bid=’002’"""

### DSL
Student.join(Borrow, ["sid"]) \
       .select("sid", "sname") \
       .filter(F.col("bid") == "0002") \
       .show(truncate = False)

# En SQL
spark.sql('''select StudentSQL.sid, sname 
                from StudentSQL 
                join borrowSQL
                on StudentSQL.sid = borrowSQL.sid
                where borrowSQL.bid = '0002' ''').show(truncate = False)

"""### 4-Trouver les titres de tous les livres empruntés par des étudiants en Mécanique (département Mécanique)"""

### DSL
Student.join(Borrow, ["sid"]) \
       .join(Book, ["bid"]) \
       .select("sid", "dept", "title") \
       .filter(F.col("dept") == "Mécanique") \
       .orderBy(F.col('bid')) \
       .show(truncate = False)

# En SQL
spark.sql('''select StudentSQL.sid, dept, title
                from BookSQL, borrowSQL, StudentSQL
                where BookSQL.bid = borrowSQL.bid and borrowSQL.sid = StudentSQL.sid
                and dept = 'Mécanique' ''').show(truncate = False)

"""### 5-Trouver les étudiants qui n’ont jamais emprunté de livre. 

"""

### DSL
Student.join(Borrow, ["sid"], "left_anti") \
    .select("*") \
    .show(truncate = False)

# En SQL
spark.sql('''select sid, sname, dept 
                from StudentSQL 
                left anti join borrowSQL
                on StudentSQL.sid = borrowSQL.sid''').show(truncate = False)

"""### 6- Déterminer l’auteur qui a écrit le plus de livres. """

### DSL
Author.join(Write, ["aid"]) \
      .join(Book, ['bid']) \
      .select("*") \
      .distinct() \
      .groupBy("name") \
      .agg(F.count("bid").alias("nombre")) \
      .limit(1) \
      .show(truncate = False)

# En SQL
spark.sql(''' select name, count(distinct bid) as nombre
                from AuthorSQL, writeSQL
                where AuthorSQL.aid = writeSQL.aid
                group by name 
                order by nombre desc limit 1''').show(truncate = False)

"""### 7- Déterminer les personnes qui n’ont pas encore rendu les livres."""

### DSL
Student.join(Borrow, ["sid"]) \
       .select("*") \
       .filter(F.col("return_time") == "null") \
       .show(truncate = False)

# En SQL
spark.sql(''' select * 
                from StudentSQL 
                join borrowSQL
                on StudentSQL.sid = borrowSQL.sid
                where return_time = 'null' ''').show(truncate = False)

"""### 8- Créer une nouvelle colonne dans la table borrow et l'exporter dans le dossier contention"""

# Utiliser le fichier de configuration pour récupérer les path. 
config = configparser.ConfigParser()
config.read('Properties/properties.conf')

path_to_output_data = config['BDA-SQL']['Output-data']

### DSL
Borrow.withColumn(
                  "Statut",
                  F.when((F.datediff(F.to_date("return_time", "dd-MM-yyyy"),
                                     F.to_date("checkout_time", "dd-MM-yyyy")) > 90 ) 
                        | (F.col("return_time") == "null"), 1)
                   .otherwise(0)) \
      .show(truncate = False)

# En SQL
spark.sql(''' select *,
                CASE
                  when datediff(to_date(return_time, 'dd-MM-yyyy'), to_date(checkout_time, 'dd-MM-yyyy')) > 90 then '1'
                  when return_time = 'null' and datediff(DATE(NOW()), to_date(checkout_time, 'dd-MM-yyyy')) > 90 then '1'
                  else '0'
                END as Statut
                from borrowSQL ''').show(truncate = False)

# Exportation des données en CSV dans un répertoire nommé contention
spark.sql(''' select *,
                CASE
                  when datediff(to_date(return_time, 'dd-MM-yyyy'), to_date(checkout_time, 'dd-MM-yyyy')) > 90 then '1'
                  when return_time = 'null' and datediff(DATE(NOW()), to_date(checkout_time, 'dd-MM-yyyy')) > 90 then '1'
                  else '0'
                END as Statut
                from borrowSQL ''').toPandas().to_csv(path_to_output_data + 'data.csv')

"""### 9-déterminer les livres qui n’ont jamais été empruntés. """

### DSL
Book.join(Borrow, ["bid"], how="left_anti") \
    .select("*")\
    .show(truncate = False)

# En SQL
spark.sql('''select *
                from BookSQL 
                left anti join borrowSQL
                on BookSQL.bid = borrowSQL.bid''').show(truncate = False)

#### Fermeture de spark
spark.stop()