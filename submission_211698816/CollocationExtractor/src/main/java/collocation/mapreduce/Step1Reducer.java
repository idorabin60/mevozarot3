package collocation.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class Step1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable result = new LongWritable();
    private MultipleOutputs<Text, LongWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        
        String keyStr = key.toString();
        // Check if this is an "N" record
        if (keyStr.contains("\tN\t")) {
            // Write to "counters" output
            // Output Key: decade \t N \t *
            // Output Value: sum
            mos.write("counters", key, result);
        } else {
            // Standard output
            context.write(key, result);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
