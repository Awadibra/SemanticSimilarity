import java.io.IOException;


import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step0 {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text value, Context context) throws IOException,  InterruptedException {
            String[] split = value.toString().split("\t");
            context.write(new Text(split[0]),new Text(split[1]));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        HashSet<String> set;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            set = new HashSet<>();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String word = key.toString();
            if(!set.contains(word)){
                set.add(word);
                context.write(new Text(word),new Text(""));
            }
            for(Text value : values){
                word = value.toString();
                if(!set.contains(word)) {
                    set.add(word);
                    context.write(new Text(word), new Text(""));
                }
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step0");
        job.setJarByClass(Step0.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}