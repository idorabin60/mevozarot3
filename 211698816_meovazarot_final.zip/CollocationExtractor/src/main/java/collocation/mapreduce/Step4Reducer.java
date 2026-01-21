package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Step4Reducer extends Reducer<Text, Text, Text, Text> {

    private final int K = 100;
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        PriorityQueue<CollocationResult> heap = new PriorityQueue<>();
        String decade = key.toString();

        for (Text val : values) {
            String s = val.toString();
            String[] parts = s.split("\t");
            if (parts.length < 2)
                continue;

            double llr = Double.parseDouble(parts[0]);
            String words = parts[1];

            heap.add(new CollocationResult(decade, llr, words));

            if (heap.size() > K) {
                heap.poll();
            }
        }

        // Output in descending order
        List<CollocationResult> results = new ArrayList<>();
        while (!heap.isEmpty()) {
            results.add(heap.poll());
        }
        Collections.sort(results, (a, b) -> Double.compare(b.llr, a.llr)); // Descending

        for (CollocationResult res : results) {
            outKey.set(decade);
            outValue.set(res.words + "\t" + res.llr);
            context.write(outKey, outValue);
        }
    }
}
