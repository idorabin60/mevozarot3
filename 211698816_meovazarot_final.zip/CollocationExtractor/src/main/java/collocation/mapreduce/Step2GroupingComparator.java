package collocation.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Step2GroupingComparator extends WritableComparator {
    protected Step2GroupingComparator() {
        super(KeyStep2.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyStep2 k1 = (KeyStep2) a;
        KeyStep2 k2 = (KeyStep2) b;

        // Group by (decade, word) ignoring type
        int cmp = k1.decadeKey.compareTo(k2.decadeKey);
        if (cmp != 0)
            return cmp;

        return k1.word.compareTo(k2.word);
    }
}
