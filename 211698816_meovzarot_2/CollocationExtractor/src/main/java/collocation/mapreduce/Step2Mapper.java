package collocation.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step2Mapper extends Mapper<Object, Text, KeyStep2, Text> {

    // Input format from Step 1 Reducer: "decade \t type \t w1 [w2] \t count"
    // Types: U (unigram), B (bigram)

    private KeyStep2 outKey = new KeyStep2();
    private Text outValue = new Text(); // Holds payload to join

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        // Expected: decade, type, w1...
        if (parts.length < 4)
            return;

        String decade = parts[0];
        String type = parts[1];

        if (type.equals("U")) {
            // Unigram: decade \t U \t w1 \t count
            String w1 = parts[2];
            String count = parts[3];

            outKey.decadeKey.set(decade);
            outKey.word.set(w1);
            outKey.type = 0; // First

            outValue.set("U:" + count);
            context.write(outKey, outValue);

        } else if (type.equals("B")) {
            // Bigram: decade \t B \t w1 w2 \t count
            // w1 w2 is in one field? In Step 1 Mapper I emitted "w1 + " " + w2".
            // Step 1 Reducer output Key "decade \t B \t w1 w2"
            // So parts split by TAB might split "w1 w2" if I was careless?
            // In Step 1 Mapper key was: decade + "\tB\t" + w1 + " " + w2
            // So Reducer output line is: "decade \t B \t w1 w2 \t count"
            // If I split by "\t", I get:
            // [0]=decade, [1]=B, [2]="w1 w2", [3]=count

            String w1w2 = parts[2];
            String count = parts[3];

            String[] words = w1w2.split(" ");
            if (words.length != 2)
                return;
            String w1 = words[0];
            String w2 = words[1];

            outKey.decadeKey.set(decade);
            outKey.word.set(w1);
            outKey.type = 1; // After U

            outValue.set("B:" + w2 + ":" + count);
            context.write(outKey, outValue);
        }
    }
}
