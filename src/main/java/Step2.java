import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Step2 {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable rowNumber, Text value, Context context) throws IOException,  InterruptedException {
            String[] split = value.toString().split("\t");
//            String[] vals = split[1].split("$");
            String word = split[0];
//            String sum = vals[0];
//            for(int i = 1; i<vals.length; i++){
                String[] occFeat = split[1].split(":");
                String occ = occFeat[0];
                for (int j = 1; j< occFeat.length; j++){
                    String feature = occFeat[j];
                    context.write(new Text(word), new Text(feature+":"+occ));
                    //      dog:sum   bite-verb:70
                }
//            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,MapWritable> {
        MapWritable map;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            map = new MapWritable();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values){
                String[] vals = value.toString().split(":");
                Text feature = new Text(vals[0]);
                long occVal = Long.parseLong(vals[1]);
                LongWritable occ = new LongWritable(occVal);
                if(map.containsKey(feature)){
                    LongWritable temp = (LongWritable) map.get(feature);
                    long newVal = occVal + temp.get();
                    map.put(feature, new LongWritable(newVal));
                }
                else{
                    map.put(feature, occ);
                }
            }
            context.write(key, map);
            map.clear();
            // alligator:sum    map{feature:occ,...}
            //alligator:55      map{bite-sub:10}

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}