package collocation;

import collocation.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main extends org.apache.hadoop.conf.Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Main <1gram path> <2gram path> <output path> <stopwords path>");
            System.exit(1);
        }
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath1gram = args[0];
        String inputPath2gram = args[1];
        String outputPath = args[2];
        String stopWordsPath = args[3];

        String step1Output = outputPath + "/step1";
        String step2Output = outputPath + "/step2";
        String step3Output = outputPath + "/step3";
        String finalOutput = outputPath + "/final";

        Configuration conf = getConf();
        // Setup local aggregation toggle if needed
        // conf.set("use.local.aggregation", "true");

        // --- FIX: Get FileSystem from the Path, not generic default ---
        Path outputDir = new Path(outputPath);
        FileSystem fs = outputDir.getFileSystem(conf);

        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        // -------------------------------------------------------------

        // Job 1: Aggregation
        Job job1 = Job.getInstance(conf, "Step 1: Aggregation");
        job1.setJarByClass(Main.class);

        // Add Stop Words to Distributed Cache
        job1.addCacheFile(new java.net.URI(stopWordsPath));

        // Note: MultipleInputs sets mapper per input
        job1.setReducerClass(Step1Reducer.class);
        job1.setCombinerClass(Step1Combiner.class); // Use Dedicated Combiner
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        MultipleInputs.addInputPath(job1, new Path(inputPath1gram), SequenceFileInputFormat.class, Step1Mapper.class);
        MultipleInputs.addInputPath(job1, new Path(inputPath2gram), SequenceFileInputFormat.class, Step1Mapper.class);

        FileOutputFormat.setOutputPath(job1, new Path(step1Output));
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Add MultipleOutputs for Counters
        MultipleOutputs.addNamedOutput(job1, "counters",
                TextOutputFormat.class, Text.class, LongWritable.class);

        if (!job1.waitForCompletion(true)) {
            return 1;
        }
        // Read N from file (Persisted Counters)
        readDecadeCounts(conf, new Path(step1Output), fs);

        // Job 2: Join C(w1)
        Job job2 = Job.getInstance(conf, "Step 2: Join C(w1)");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(Step2Mapper.class);
        job2.setReducerClass(Step2Reducer.class);
        job2.setPartitionerClass(Step2Partitioner.class);
        job2.setGroupingComparatorClass(Step2GroupingComparator.class);

        job2.setMapOutputKeyClass(KeyStep2.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(step1Output));
        job2.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job2, new Path(step2Output));

        if (!job2.waitForCompletion(true)) {
            return 1;
        }

        // Job 3: Join C(w2) + LLR
        Job job3 = Job.getInstance(conf, "Step 3: Join C(w2) + LLR");
        job3.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job3, new Path(step1Output), TextInputFormat.class, Step3MapperUnigram.class);
        MultipleInputs.addInputPath(job3, new Path(step2Output), TextInputFormat.class, Step3MapperJoin.class);

        job3.setReducerClass(Step3Reducer.class);
        job3.setPartitionerClass(Step2Partitioner.class);
        job3.setGroupingComparatorClass(Step2GroupingComparator.class);

        job3.setMapOutputKeyClass(KeyStep2.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job3, new Path(step3Output));

        if (!job3.waitForCompletion(true)) {
            return 1;
        }

        // Job 4: Sort
        Job job4 = Job.getInstance(conf, "Step 4: Top 100 Sort");
        job4.setJarByClass(Main.class);
        job4.setMapperClass(Step4Mapper.class);
        job4.setReducerClass(Step4Reducer.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, new Path(step3Output));
        job4.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job4, new Path(finalOutput));

        return job4.waitForCompletion(true) ? 0 : 1;
    }

    private void readDecadeCounts(Configuration conf, Path outputDir, FileSystem fs) throws Exception {
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> files = fs.listFiles(outputDir,
                false);
        while (files.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus status = files.next();
            String name = status.getPath().getName();
            if (name.startsWith("counters-r-")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    // Line: decade \t N \t * \t count
                    String[] parts = line.split("\t");
                    if (parts.length >= 4 && parts[1].equals("N")) {
                        String decade = parts[0];
                        String count = parts[3];
                        conf.set("N_" + decade, count);
                        System.out.println("Loaded N for " + decade + ": " + count);
                    }
                }
                br.close();
            }
        }
    }
}