package hit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HitMapper extends Mapper<LongWritable, Text, // Ne pas modifier !
        Text, IntWritable> // Couple (key, value) en sortie du mapper
{
    // key est l'offset, adresse relative depuis le début du fichier (Ne pas modifier !).
// value est le texte à mapper (Ne pas modifier !).
    public void map (LongWritable key, Text value, Context context)
            throws	IOException, InterruptedException {

        // Transforme l'objet 'Text' en chaîne de caractères.
        String line = value.toString();

        // Découpe la chaîne de caractères.
        Pattern p = Pattern.compile(":\\d\\d:\\d\\d:"); // capture :HH:MM:
        Matcher m = p.matcher(line);

        if (m.find()) {
            String word = m.group();
            int min = Integer.parseInt(word.substring(4,6));
            int newMin = (min / 15) * 15;
            String newWord = word.substring(1,3) + " : " + Integer.toString(newMin);

            // Ecriture du couple clé/valeur en sortie du mapper.
            context.write(new Text(newWord), new IntWritable(1));
        }



    }
}

