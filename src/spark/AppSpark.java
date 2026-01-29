package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

public class AppSpark
{
    /**
     * Programme principal.
     */
    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("AppSpark")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark version : " + spark.version());
        System.out.println("---------- AppSpark (code Java) ----------");

        // DF Arbres.csv
        Dataset<Row> arbresDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv("hdfs://namenode:9000/user/root/arbres/les-arbres.csv");

        // Exo : Nombre abricotier
        long nbAbri = arbresDF
                .filter(col("LIBELLE FRANCAIS").equalTo("Abricotier"))
                .count();

        // Exo : Nombre abricotier (SQL)
        arbresDF.createOrReplaceTempView("Arbres");

        Dataset<Row> sqlNbAbri = spark.sql(
                "SELECT COUNT(*) AS nb_abricotiers " +
                        "FROM Arbres " +
                        "WHERE `LIBELLE FRANCAIS` = 'Abricotier'"
        );

        // Exo : Arbre le plus présent dans les écoles
        Dataset<Row> nbArbreParEcole = arbresDF
                .filter(col("LIEU / ADRESSE").like("ECOLE%"))
                .groupBy("LIBELLE FRANCAIS")
                .count();

        Dataset<Row> mostArbreEcole = nbArbreParEcole
                .orderBy(col("count").desc());

        Dataset<Row> ArbreCount = arbresDF
                .filter(col("LIBELLE FRANCAIS").isin("Tilleul", "Platane", "Peuplier", "Bouleau"))
                .groupBy("LIBELLE FRANCAIS")
                .count();

        // Exo : Arbre le plus présent dans les écoles (SQL)
        Dataset<Row> sqlArbreCount = spark.sql(
                "SELECT `LIBELLE FRANCAIS`, COUNT(*) AS nb_arbres " +
                        "FROM Arbres " +
                        "WHERE `LIBELLE FRANCAIS` IN ('Tilleul', 'Platane', 'Peuplier', 'Bouleau')" +
                        "GROUP BY `LIBELLE FRANCAIS`"
        );

        // Exo : Moyenne et circonferences des arbres
        Dataset<Row> arbresTaille = arbresDF
                .filter(col("LIBELLE FRANCAIS").isin("Tilleul", "Platane", "Peuplier", "Bouleau"))
                .filter(col("CIRCONFERENCE (cm)").gt(0))
                .filter(col("HAUTEUR (m)").gt(0))
                .groupBy("LIBELLE FRANCAIS")
                .agg(functions.avg("CIRCONFERENCE (cm)").alias("CIRCONFERENCE_MOYENNE"),functions.avg("HAUTEUR (m)").alias("HAUTEUR_MOYENNE"));

        // Exemple GROUPBY
        Dataset<Row> nbArbreParType = arbresDF
                .groupBy("LIBELLE FRANCAIS")  // Regroupe par type d'arbre
                .count();                     // Compte le nombre de lignes par groupe

        //Exemple
        Dataset<Row> statsArbre = arbresDF
                .filter(col("HAUTEUR (m)").gt(0))
                .groupBy("LIBELLE FRANCAIS")
                .agg(
                        avg("HAUTEUR (m)").alias("Hauteur_moyenne"),
                        max("HAUTEUR (m)").alias("Hauteur_max"),
                        min("HAUTEUR (m)").alias("Hauteur_min")
                );


        // DF Velib.csv
        Dataset<Row> velibDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv("hdfs://namenode:9000/user/root/velib/velib.csv");

        // DF evenement.csv
        Dataset<Row> evenementDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .csv("hdfs://namenode:9000/user/root/evenement/evenement.csv");

        Dataset<Row> events = evenementDF
                .withColumn("Date de début",
                        to_date(split(col("Date de début"), "T").getItem(0)))
                .withColumn("Date de fin",
                        to_date(split(col("Date de fin"), "T").getItem(0)));


        int mois = 8;
        int annee = 2025;

        LocalDate target = LocalDate.of(annee, mois, 1);

        Dataset<Row> filteredEvents = events.filter(
                col("Date de début").leq(lit(target))
                        .and(col("Date de fin").geq(lit(target)))
        );


        //System.out.println(nbAbri);
        //mostArbreEcole.show(1,false);

        //ArbreCount.show();
        //arbresTaille.show();
        //sqlNbAbri.show();
        //      sqlArbreCount.show();
        filteredEvents.select("Date de début", "Titre").show(false);

        /*----- Arrêt de la session -----*/
        spark.stop();

    }

} /*----- Fin de la classe -----*/


