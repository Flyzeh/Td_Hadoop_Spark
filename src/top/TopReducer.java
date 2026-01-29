package top;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReducer extends Reducer<Text, Text, // Couple (key, value) en sortie du mapper
        Text, Text> // Couple (key, value) en sortie du reducer
{
    // key est la clé extraite du mapper.
// values est le liste des valeurs issues des mappers pour chaque occurrence de key.
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Map pour stocker quantité totale par produit
        Map<String, Integer> productCounts = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split("=");
            String product = parts[0];
            int qty = Integer.parseInt(parts[1]);

            productCounts.put(product, productCounts.getOrDefault(product, 0) + qty);
        }

        // PriorityQueue pour trier les produits par quantité décroissante
        PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<>(
                Comparator.comparingInt(Map.Entry::getValue)
        );

        // On garde uniquement les top 3
        for (Map.Entry<String, Integer> entry : productCounts.entrySet()) {
            pq.offer(entry);
            if (pq.size() > 3) pq.poll(); // retire le plus petit
        }

        // Construire la chaîne de sortie Top-3
        StringBuilder sb = new StringBuilder();
        while (!pq.isEmpty()) {
            Map.Entry<String, Integer> entry = pq.poll();
            sb.insert(0, entry.getKey() + "(" + entry.getValue() + ") "); // insert devant pour ordre décroissant
        }

        context.write(key, new Text(sb.toString().trim()));
    }
}

