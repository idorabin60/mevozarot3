package collocation.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A simple combiner that just sums up counts.
 * It does NOT write to MultipleOutputs, ensuring that N-records
 * are correctly passed on to the final Reducer.
 */
public class Step1Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable result = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
