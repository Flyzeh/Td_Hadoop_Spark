package index;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, IntWritable, // Couple (key, value) en sortie du mapper
        Text, Text> // Couple (key, value) en sortie du reducer
{
    // key est la clé extraite du mapper.
// values est le liste des valeurs issues des mappers pour chaque occurrence de key.
    public void reduce (Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        String index = "";
        boolean first = true;

        // On compte le nombre de valeurs.
        for (IntWritable value : values) {
            if (!first)
                // sb.append(",")
                index += ",";
            index += Integer.toString(value.get());
            //sb.append(value.get());
            first = false;
        }

        // Ecriture du résultat
        context.write(key, new Text(index));
    }
}

