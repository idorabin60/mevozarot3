package collocation.mapreduce;

import collocation.input.StopWords;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // Output Key: "decade \t [U|B] \t w1 [w2]"
    // Output Value: count

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load stop words from Distributed Cache
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            for (java.net.URI uri : context.getCacheFiles()) {
                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(uri.getPath());
                String fileName = path.getName();

                if (fileName.contains("stopwords")) {

                    java.io.File f = new java.io.File(fileName);
                    if (f.exists()) {
                        StopWords.load(fileName);
                    } else {
                        StopWords.load(path.toString());
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Format: ngram TAB year TAB match_count TAB ...
        String[] parts = line.split("\t");

        if (parts.length < 3)
            return; // Malformed

        String ngram = parts[0];
        String yearStr = parts[1];
        String countStr = parts[2];

        int year;
        long count;
        try {
            year = Integer.parseInt(yearStr);
            count = Long.parseLong(countStr);
        } catch (NumberFormatException e) {
            return;
        }

        int decade = (year / 10) * 10;
        String decadeStr = String.valueOf(decade);

        String[] words = ngram.split(" ");

        // Unigram check
        if (words.length == 1) {
            String w = words[0].trim();
            if (StopWords.contains(w))
                return;

            // Emit Unigram
            outKey.set(decadeStr + "\tU\t" + w);
            outValue.set(count);
            context.write(outKey, outValue);

            // Emit Decade Total Count (N)
            // Key: "decade \t N \t *"
            // Value: count
            // The Combiner (Step1Reducer) will sum acts on this key too.
            outKey.set(decadeStr + "\tN\t*");
            outValue.set(count);
            context.write(outKey, outValue);

        } else if (words.length == 2) {
            String w1 = words[0].trim();
            String w2 = words[1].trim();

            if (StopWords.contains(w1) || StopWords.contains(w2))
                return;

            // Emit Bigram
            outKey.set(decadeStr + "\tB\t" + w1 + " " + w2);
            outValue.set(count);
            context.write(outKey, outValue);
        }
    }
}
