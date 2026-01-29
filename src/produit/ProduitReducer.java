package produit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProduitReducer extends Reducer<Text, Text, // Couple (key, value) en sortie du mapper
        Text, IntWritable> // Couple (key, value) en sortie du reducer
{
    // key est la clé extraite du mapper.
// values est le liste des valeurs issues des mappers pour chaque occurrence de key.
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Set<String> clients = new HashSet<>();
        for (Text val : values) {
            clients.add(val.toString()); // stocke les clients distincts
        }

        context.write(key, new IntWritable(clients.size()));
    }
}

