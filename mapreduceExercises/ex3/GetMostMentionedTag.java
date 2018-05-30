package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GetMostMentionedTag {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Create a new job
        Job job = Job.getInstance(conf, "GetMostMentionedTag");

        job.setNumReduceTasks(1);
        // Use the WordCount.class file to point to the job jar
        job.setJarByClass(GetMostMentionedTag.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Setting the input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for it's completion
        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String tag = fields[2];

            word.set(tag);
            context.write(word, one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int mostCitedTagCounter = 0;
        private String mostCitedTag = null;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int numberOfCites = 0;
            for (IntWritable value : values) {
                numberOfCites += value.get();
            }

            if (numberOfCites > mostCitedTagCounter) {
                mostCitedTagCounter = numberOfCites;
                mostCitedTag = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
          context.write(new Text(mostCitedTag), new IntWritable(mostCitedTagCounter));
        }
    }
}

