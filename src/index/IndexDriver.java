package index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexDriver {

    public static void main(String[] args) throws Exception {

        // Vérification de la présence des 3 arguments de la fonction.
        if (args.length != 3) {
            System.out.printf("Format de la ligne de commande IndexDriver <rep source> <rep cible>\n");
            System.exit(-1);
        }

        // Instanciation et initialisation du job Hadoop.
        Job job = Job.getInstance();
        job.setJarByClass(IndexDriver.class); // Classe de lancement.
        job.setJobName("Index"); // Nom du job.
        FileInputFormat.setInputPaths(job, new Path(args[0])); // Chemin du répertoire d'entrée.
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Chemin du répertoire de sortie.

        // Ajout de mot à indexer au contexte des jobs.
        job.getConfiguration().set("mot_a_indexer", args[2]);

        // Indication au job des classes à exécuter pour le mapper et le reducer.
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        // Indication des types pour le couple (key, value) en sortie du mapper.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Indication des types pour le couple (key, value) en sortie du reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Lancement du job.
        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }
}

