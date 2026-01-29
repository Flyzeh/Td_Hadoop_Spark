package an;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnReducer extends Reducer<Text, Text, // Couple (key, value) en sortie du mapper
        Text, Text> // Couple (key, value) en sortie du reducer
{
    // key est la clé extraite du mapper.
// values est le liste des valeurs issues des mappers pour chaque occurrence de key.
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        HashSet<String> NoDuplicate = new HashSet<>();

        // Enleve doublons
        for (Text val : values) {
            NoDuplicate.add(val.toString());
        }

        // Ne conserver que les vrais anagrammes
        if (NoDuplicate.size() > 1) {
            StringBuilder sb = new StringBuilder();
            for (String word : NoDuplicate) {
                sb.append(word).append(" ");
            }

            context.write(key, new Text(sb.toString().trim()));
        }
    }
}
