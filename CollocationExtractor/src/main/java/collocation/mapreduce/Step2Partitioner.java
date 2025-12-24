package collocation.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class Step2Partitioner extends Partitioner<KeyStep2, Text> {
    @Override
    public int getPartition(KeyStep2 key, Text value, int numPartitions) {
        // Partition by (decade, word) only
        // So that (1990, apple, 0) and (1990, apple, 1) go to same reducer
        return (key.decadeKey.hashCode() * 31 + key.word.hashCode()) & Integer.MAX_VALUE % numPartitions;
    }
}
