package an;

import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnMapper extends Mapper<LongWritable, Text, // Ne pas modifier !
        Text, Text> // Couple (key, value) en sortie du mapper
{
    // key est l'offset, adresse relative depuis le début du fichier (Ne pas modifier !).
// value est le texte à mapper (Ne pas modifier !).
    public void map (LongWritable key, Text value, Context context)
            throws	IOException, InterruptedException {

        // Transforme l'objet 'Text' en chaîne de caractères.
        String line = value.toString();

        // Découpe la chaîne de caractères.
        for (String word : line.split("[^\\p{L}]+"))
            if (word.length() > 0) {

                String lower = word.toLowerCase();

                String normalized = Normalizer.normalize(lower, Normalizer.Form.NFD)
                        .replaceAll("\\p{M}", "");

                //Crée une liste avec chaque lettre du mot
                ArrayList<Character> lettres = new ArrayList<>();

                //Rempli la liste
                for (int i = 0; i < word.length(); i++) {
                    lettres.add(word.charAt(i));
                }

                //Trie la liste
                Collections.sort(lettres);

                //Transforme la liste en String
                StringBuilder sb = new StringBuilder();
                for (char c : lettres) {
                    sb.append(c);
                }

                // Ecriture du couple clé/valeur en sortie du mapper.
                context.write(new Text(sb.toString()), new Text(word));
            }
    }
}
