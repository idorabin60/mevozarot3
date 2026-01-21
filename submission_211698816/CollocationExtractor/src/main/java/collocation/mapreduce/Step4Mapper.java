package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class Step4Mapper extends Mapper<Object, Text, Text, Text> {

    private Map<String, PriorityQueue<CollocationResult>> heaps = new HashMap<>();
    private final int K = 100;

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 3)
            return;

        String decade = parts[0];
        double llr;
        try {
            llr = Double.parseDouble(parts[1]);
        } catch (NumberFormatException e) {
            return;
        }
        String words = parts[2]; // "w1 w2"

        heaps.putIfAbsent(decade, new PriorityQueue<>());
        PriorityQueue<CollocationResult> heap = heaps.get(decade);

        heap.add(new CollocationResult(decade, llr, words));

        if (heap.size() > K) {
            heap.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, PriorityQueue<CollocationResult>> entry : heaps.entrySet()) {
            String decade = entry.getKey();
            PriorityQueue<CollocationResult> heap = entry.getValue();

            for (CollocationResult res : heap) {
                outKey.set(decade);
                outValue.set(res.llr + "\t" + res.words);
                context.write(outKey, outValue);
            }
        }
    }
}
