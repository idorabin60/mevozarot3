package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step3MapperUnigram extends Mapper<Object, Text, KeyStep2, Text> {

    // Reads Step 1 output: decade \t U \t w \t count

    private KeyStep2 outKey = new KeyStep2();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 4)
            return;

        String decade = parts[0];
        String type = parts[1];

        if ("U".equals(type)) {
            String w = parts[2];
            String count = parts[3];

            // Key: (decade, w, 0) -> this is now "C(w2)" role
            outKey.decadeKey.set(decade);
            outKey.word.set(w);
            outKey.type = 0;

            outValue.set("U:" + count);
            context.write(outKey, outValue);
        }
    }
}
