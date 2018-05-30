package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AverageRating {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Create a new job
        Job job = Job.getInstance(conf, "AverageRating");

        // Use the WordCount.class file to point to the job jar
        job.setJarByClass(AverageRating.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

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

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text movie = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            String movieId = fields[1];
            float rating = Float.parseFloat(fields[2]);

            movie.set(movieId);
            context.write(movie, new FloatWritable(rating));
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0F;
            int count = 0;
            for (FloatWritable value : values) {
                sum += valuei.get();
                count++;
            }

            context.write(key, new FloatWritable(sum/count));
        }
    }
}

