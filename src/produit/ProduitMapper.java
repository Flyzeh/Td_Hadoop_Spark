package produit;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProduitMapper extends Mapper<LongWritable, Text, // Ne pas modifier !
        Text, Text> // Couple (key, value) en sortie du mapper
{
    // key est l'offset, adresse relative depuis le début du fichier (Ne pas modifier !).
// value est le texte à mapper (Ne pas modifier !).
    public void map (LongWritable key, Text value, Context context)
            throws	IOException, InterruptedException {

        // Transforme l'objet 'Text' en chaîne de caractères.
        String line = value.toString();
        String[] parts = line.split(";");

        if (parts.length < 3) return;

        String clientId = parts[1];
        String[] products = Arrays.copyOfRange(parts, 2, parts.length);

        for (int i = 0; i < products.length; i++) {
            for (int j = i + 1; j < products.length; j++) {
                String prod1 = products[i];
                String prod2 = products[j];

                // Toujours trier pour éviter doublons (A,B) et (B,A)
                String pairKey = prod1.compareTo(prod2) < 0 ? prod1 + "," + prod2 : prod2 + "," + prod1;
                context.write(new Text(pairKey), new Text(clientId));
            }
        }
    }
}

