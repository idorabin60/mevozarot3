package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step2Reducer extends Reducer<KeyStep2, Text, Text, Text> {

    // Output: decade \t w1 \t w2 \t cw1 \t cw1w2

    private Text outKey = new Text();
    private Text outValue = new Text(); // Just the rest of fields to keep TextOutputFormat simple?
    // Or key=decade, value=...

    @Override
    protected void reduce(KeyStep2 key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Due to sorting: U record comes first
        long cw1 = -1;

        for (Text val : values) {
            String s = val.toString();
            if (s.startsWith("U:")) {
                // Found C(w1)
                cw1 = Long.parseLong(s.substring(2));
            } else if (s.startsWith("B:")) {
                // Bigram record: B:w2:count
                if (cw1 == -1) {
                    // No unigram found for w1? Possible if w1 was stopword filtered but bigram
                    // wasn't?
                    // We filtered bigrams if EITHER word is stopword.
                    // So we should have C(w1) unless corpus error or filtered some other way.
                    // Or maybe w1 appearing in bigram but never as unigram (possible in n-gram
                    // datasets if pruned diff).
                    // Skip or ignore.
                    continue;
                }

                String[] parts = s.split(":");
                String w2 = parts[1];
                String cw1w2 = parts[2];

                // key.decadeKey, key.word is w1
                String decade = key.decadeKey.toString();
                String w1 = key.word.toString();

                // Output: decade \t w1 \t w2 \t cw1 \t cw1w2
                // We can use NullWritable value or Text key
                outKey.set(decade + "\t" + w1 + "\t" + w2);
                outValue.set(cw1 + "\t" + cw1w2);
                context.write(outKey, outValue);
            }
        }
    }
}
