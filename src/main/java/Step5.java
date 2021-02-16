import java.io.DataInputStream;
import java.io.IOException;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step5 {

    public static class MapperClass extends Mapper<Text,MapWritable,Text,MapWritable> {
        MapWritable map;
        //v1
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            map = new MapWritable();
        }

        @Override
        public void map(Text key, MapWritable value, Context context) throws IOException,  InterruptedException {
            String[] split = value.toString().split("\t");
            map = value;
            for(Map.Entry<Writable,Writable> in : map.entrySet()){
                System.out.println(in.getKey().toString() + " " + in.getValue().toString());
            }
            context.write(key,map);
            //alligatorXdog alligatorVector
            //dogXalligator alligatorVector
            // to be continued...
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,MapWritable,Text,MapWritable> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException,  InterruptedException {

            for(MapWritable value : values){
                context.write(key,value);
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step5");
        job.setJarByClass(Test2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}