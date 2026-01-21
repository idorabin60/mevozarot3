package collocation.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Step3Reducer extends Reducer<KeyStep2, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(KeyStep2 key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long cw2 = -1;
        String decade = key.decadeKey.toString();

        Configuration conf = context.getConfiguration();
        String nStr = conf.get("N_" + decade);

        if (nStr == null || nStr.isEmpty()) {
            return;
        }

        long N;
        try {
            N = Long.parseLong(nStr);
        } catch (NumberFormatException e) {
            return; // Skip if N is corrupted
        }

        for (Text val : values) {
            String s = val.toString();

            // 2. Wrap parsing in try-catch to handle dirty English data
            try {
                if (s.startsWith("U:")) {
                    String numberPart = s.substring(2);
                    if (!numberPart.isEmpty()) {
                        cw2 = Long.parseLong(numberPart);
                    }
                } else if (s.startsWith("P:")) {
                    if (cw2 == -1)
                        continue;

                    String[] parts = s.split(":");

                    if (parts.length < 4)
                        continue;

                    String w1 = parts[1];
                    String cw1Str = parts[2];
                    String cw1w2Str = parts[3];

                    // Fix: Check for empty strings before parsing
                    if (cw1Str.isEmpty() || cw1w2Str.isEmpty())
                        continue;

                    long cw1 = Long.parseLong(cw1Str);
                    long cw1w2 = Long.parseLong(cw1w2Str);
                    String w2 = key.word.toString();

                    double llr = LLR.calculate(cw1, cw2, cw1w2, N);

                    outKey.set(decade);
                    outValue.set(String.valueOf(llr) + "\t" + w1 + " " + w2);
                    context.write(outKey, outValue);
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Ignore malformed lines (bad data in English corpus)
                continue;
            }
        }
    }
}