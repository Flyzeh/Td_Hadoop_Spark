package top;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopMapper extends Mapper<LongWritable, Text, // Ne pas modifier !
        Text, Text> // Couple (key, value) en sortie du mapper
{
    // key est l'offset, adresse relative depuis le début du fichier (Ne pas modifier !).
// value est le texte à mapper (Ne pas modifier !).
    public void map (LongWritable key, Text value, Context context)
            throws	IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split(";");

        if (parts.length != 3) return;

        String clientId = parts[0];
        String product = parts[1];
        String quantity = parts[2];

        // On envoie client comme clé, et product=quantity comme valeur
        context.write(new Text(clientId), new Text(product + "=" + quantity));
    }
}

