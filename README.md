# Etapes Terminal

## 1 - Docker

- Localisation : Downloads\Docker-Hadoop\Docker-Hadoop
- Lancement : ```shell docker-compose up -d ```
- Copie d'un fichier vers le namenode : ```shell docker cp C:\Users\giani\Downloads\TD.csv namenode:/tmp/TD.csv```
- Envoyer un fichier du namenode vers le PC : ```shell docker cp namenode:/tmp/TD.txt C:\Users\giani\Desktop\```

## 2 - Hadoop

- Rentrer dans le namenode : ```shell docker exec -it namenode bash ```
- /tmp : Répertoire temporaire du système Linux

### Règles générales

- hdfs dfs -get <fichier_HDFS> <nom_local_que_tu_veux>

### Commandes Hadoop

- Création d'un répertoire : ```bash hadoop fs -mkdir -p /user/root/TD```
- Liste répertoire : ```bash hdfs dfs -ls```
- Mettre un fichier du namenode dans le répertoire : ```bash hdfs dfs -put /tmp/TD.csv /user/root/TD/```
- Lancer un job Hadoop : ```bash hadoop jar TD.jar TD.WcDriver gutenberg TD ``` 
    > Cela va créer un dossier TD dans /user/root, 
    > gutenberg représente le répertoire contenant le fichier .txt d'entrée
    > TD.WcDriver représente la classe exécutée
    > Créer le jar : ```shell jar cvf TD.jar TD ``` -> attention à bien être présent dans C:\Users\giani\IdeaProjects\MapReduce\out\production\MapReduce
- Vérifier le résultat : ```bash hdfs dfs -cat /user/root/TD/part-r-00000``` ou ```bash hdfs dfs -head /user/root/TD/part-r-00000```
- Récupérer le fichier : ```bash hdfs dfs -get /user/root/TD/part-r-00000 /tmp/TD.txt```

## 3 - Spark

- Lancer le terminal scala : ```bash ./opt/spark-3.1.2/bin/spark-shell``` -> être dans le namenode
- Lancer le jar avec spark : ```bash /opt/spark-3.1.2/bin/spark-submit --class spark.AppSpark \tmp\TD.jar```

# Code

## Mapper (Exemple : Wordcounts)

- Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    - KEYIN (LongWrittable) : type de la clé d’entrée (offset de la ligne dans le fichier)
    - VALUEIN (Text) : type de la valeur d’entrée (le contenu de la ligne)
    - KEYOUT (Text) : type de la clé de sortie (mot)
    - VALUEOUT (IntWritable) : type de la valeur de sortie (1 pour chaque mot)

```java
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException
```
    
Cette méthode est appelée une fois par ligne de fichier d’entrée.

    - key : position de la ligne
    - value : texte de la ligne
    - context : objet fourni par Hadoop pour écrire les sorties du mapper ou communiquer avec le framework.

### Reducer (Exemple : Wordcounts)

- - Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    - KEYIN (LongWrittable) : type de la clé d’entrée (mot venant du mapper)
    - VALUEIN (Text) : type de la valeur d’entrée (tous les 1 envoyés par le mapper)
    - KEYOUT (Text) : type de la clé de sortie (le mot final)
    - VALUEOUT (IntWritable) : type de la valeur de sortie (le nombre total d’occurrences du mot)

```java
public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException
```
Cette méthode est appelée une fois par clé unique après que Hadoop ait regroupé toutes les valeurs identiques provenant des mappers

    - key : le mot à compter
    - values : toutes les valeurs associées à ce mot
    - context : objet Hadoop pour écrire les résultats finaux
