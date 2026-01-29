# Etapes Terminal

## 1 - Docker

- Localisation : Downloads\Docker-Hadoop\Docker-Hadoop
- Lancement : ```docker-compose up -d```
- Copie d'un fichier vers le namenode : ```docker cp C:\Users\giani\Downloads\TD.csv namenode:/tmp/TD.csv```
- Envoyer un fichier du namenode vers le PC : ```docker cp namenode:/tmp/TD.txt C:\Users\giani\Desktop\```

## 2 - Hadoop

- Rentrer dans le namenode : ```docker exec -it namenode bash ```
- /tmp : Répertoire temporaire du système Linux

### Règles générales

- hdfs dfs -get <fichier_HDFS> <nom_local_que_tu_veux>

### Commandes Hadoop

- Création d'un répertoire : ```hadoop fs -mkdir -p /user/root/TD```
- Liste répertoire : ```hdfs dfs -ls```
- Mettre un fichier du namenode dans le répertoire : ```hdfs dfs -put /tmp/TD.csv /user/root/TD/```
- Lancer un job Hadoop : ```hadoop jar /tmp/TD.jar TD.WcDriver gutenberg TD ``` 
    - Cela va créer un dossier TD dans /user/root, 
    - gutenberg représente le répertoire contenant le fichier .txt d'entrée
    - TD.WcDriver représente la classe exécutée
    - Créer le jar : ```jar cvf TD.jar TD ``` -> attention à bien être présent dans C:\Users\giani\IdeaProjects\MapReduce\out\production\MapReduce
- Vérifier le résultat : ```hdfs dfs -cat /user/root/TD/part-r-00000``` ou ```hdfs dfs -head /user/root/TD/part-r-00000```
- Récupérer le fichier : ```hdfs dfs -get /user/root/TD/part-r-00000 /tmp/TD.txt```
- Supprimer un fichier : ```hdfs dfs -rm -r /user/root/TD```

## 3 - Spark

- Lancer le terminal scala : ```./opt/spark-3.1.2/bin/spark-shell``` -> être dans le namenode
- Lancer le jar avec spark : ```/opt/spark-3.1.2/bin/spark-submit --class spark.AppSpark \tmp\TD.jar```

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

# Spark Shell

- Charger le répertoire Gutenberg
```
val rdd_gut = sc.textFile("hdfs://localhost:9000/user/root/gutenberg")
```

- Compter le nombre de lignes :
```
rdd_gut.count()
```

- Afficher les 3 premières lignes :
```
rdd_gut.take(3).foreach(println)
```

- Nombre de caractères total :
```
val nbChars = rdd_gut.map(line => line.length).reduce(_ + _)
nbChars
```

## WordCounts
- Transformer chaque ligne en mots (flatMap)
```
val words = rdd_gut.flatMap(line => line.split("\\W+")) // Sépare par tout caractère non alphabétique
```

- Créer les couples (word, 1)
```
val wordPairs = words.map(word => (word.toLowerCase, 1)) // Mettre en minuscule pour uniformiser
```

- Somme des occurrences (reduceByKey)
```
val wordCount = wordPairs.reduceByKey(_ + _) // Somme les valeurs pour chaque mot
```

- Afficher le résultat
```
wordCount.take(20).foreach(println) // Affiche 20 mots avec leur compte

wordCount.map{case (word, count) => (count, word)}
         .sortByKey(false)
         .take(20)
         .foreach(println)
```

- Une ligne
```
sc.textFile("hdfs://localhost:9000/user/root/gutenberg")
  .flatMap(line => line.split("\\W+"))                  // Découpe chaque ligne en mots
  .map(word => (word.toLowerCase, 1))                  // Crée le couple (mot, 1)
  .reduceByKey(_ + _)                                  // Somme les occurrences par mot
  .sortBy(_._2, false)                                 // Trie par fréquence décroissante
  .take(20)                                            // Prend les 20 mots les plus fréquents
  .foreach(println)                                    // Affiche le résultat
```

- UP
```
val rdd_clean = sc.textFile("hdfs://localhost:9000/user/root/gutenberg")
  .map(line => line
    .replaceAll("\\[.*?\\]", "")   // Supprime ce qui est entre crochets [scène, mise en scène]
    .replaceAll("(?i)Acte \\d+", "")  // Supprime "Acte 1", "Acte 2" etc. (insensible à la casse)
    .replaceAll("(?i)Page \\d+", "")  // Supprime "Page 1", "Page 2" etc.
  )
  .filter(_.trim.nonEmpty) // Supprime les lignes vides
```

## Anagramme

```
val words = rdd_clean.flatMap(line => line.split("\\W+"))
  .map(_.toLowerCase)
  .filter(_.length > 1)  // on ignore les mots d'une seule lettre

val anagrams = words
  .map(word => (word.sorted, word))       // (mot_trié, mot)
  .groupByKey()
  .mapValues(_.toSet)                     // enlève les doublons
  .filter(_._2.size > 1)                  // garde les vrais anagrammes

anagrams
  .map{case (sorted, set) => set.mkString(",")}
  .saveAsTextFile("hdfs://localhost:9000/user/root/anagrams")
```

## Hits

```
val logs = sc.textFile("hdfs://localhost:9000/user/root/logs")

val hitsPerQuarter = logs.flatMap(line => {
    val timePattern = ".*?(\\d{2}):(\\d{2}):(\\d{2}).*".r
    line match {
        case timePattern(hh, mm, ss) =>
            val hour = hh.toInt
            val minute = mm.toInt
            if(hour >= 10 && hour < 23) {
                val quarter = (minute / 15) * 15
                Some(f"$hour%02d:$quarter%02d")
            } else None
        case _ => None
    }
})
.map(q => (q, 1))
.reduceByKey(_ + _)
.sortByKey()
```

## Index

```
val targetWord = "burglary" // peut venir d’un argument

val rdd_index = rdd_clean.zipWithIndex()  // donne (ligne, index)
  .flatMap{case (line, idx) =>
      if(line.toLowerCase.contains(targetWord.toLowerCase)) Some((targetWord, idx + 1))
      else None
  }
  .groupByKey()

rdd_index
  .map{case (word, indices) => s"$word : ${indices.mkString(",")}"}
  .saveAsTextFile("hdfs://localhost:9000/user/root/index")
```
