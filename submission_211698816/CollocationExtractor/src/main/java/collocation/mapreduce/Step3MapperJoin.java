package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step3MapperJoin extends Mapper<Object, Text, KeyStep2, Text> {

    // Reads Step 2 output: decade \t w1 \t w2 \t cw1 \t cw1w2

    private KeyStep2 outKey = new KeyStep2();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        // Check fields
        if (parts.length < 5)
            return;

        String decade = parts[0];
        String w1 = parts[1];
        String w2 = parts[2];
        String cw1 = parts[3];
        String cw1w2 = parts[4];

        // Join on w2
        // Key: (decade, w2, 1)
        outKey.decadeKey.set(decade);
        outKey.word.set(w2);
        outKey.type = 1;

        // Payload: P:w1:cw1:cw1w2
        outValue.set("P:" + w1 + ":" + cw1 + ":" + cw1w2);
        context.write(outKey, outValue);
    }
}
