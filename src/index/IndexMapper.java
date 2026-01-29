package index;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, // Ne pas modifier !
        Text, IntWritable> // Couple (key, value) en sortie du mapper
{
    // key est l'offset, adresse relative depuis le début du fichier (Ne pas modifier !).
// value est le texte à mapper (Ne pas modifier !).
    public void map (LongWritable key, Text value, Context context)
            throws	IOException, InterruptedException {

        // Lecture du mot à indexer.
        String mot = context.getConfiguration().get("mot_a_indexer");

        // Transforme l'objet 'Text' en chaîne de caractères.
        String line = value.toString();

        if (line.contains(mot))
            // Ecriture du couple clé/valeur en sortie du mapper.
            context.write(new Text(mot), new IntWritable((int) key.get()));
    }
}

