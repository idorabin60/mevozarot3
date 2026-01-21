package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyStep2 implements WritableComparable<KeyStep2> {
    public Text decadeKey = new Text(); // "decade" string or "decade \t w1" prefix
    public Text word = new Text();
    public int type; // 0 for Unigram, 1 for Bigram

    public KeyStep2() {
    }

    public KeyStep2(String decade, String w1, int type) {
        this.decadeKey.set(decade);
        this.word.set(w1);
        this.type = type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decadeKey.write(out);
        word.write(out);
        out.writeInt(type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decadeKey.readFields(in);
        word.readFields(in);
        type = in.readInt();
    }

    @Override
    public int compareTo(KeyStep2 o) {
        int cmp = decadeKey.compareTo(o.decadeKey);
        if (cmp != 0)
            return cmp;

        cmp = word.compareTo(o.word);
        if (cmp != 0)
            return cmp;

        return Integer.compare(this.type, o.type);
    }

    @Override
    public String toString() {
        return decadeKey + "\t" + word + "\t" + type;
    }
}
