import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class Step3 {

    public static class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
        HashMap<String, List> pairs;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();
            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3").key("word-relatedness.txt").build(),
                    ResponseTransformer.toFile(Paths.get("pairs.txt")));
            pairs = new HashMap<>();
            File file = new File("pairs.txt");
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                String lastHead = null;
                List list = new LinkedList();
                while ((line = br.readLine()) != null) {
                    String[] split = line.split("\\s+");
                    if (lastHead == null | lastHead == split[0]){
                        list.add(split[1]);
                    }
                    else{
                        pairs.put(lastHead, list);
                        list.clear();
                        list.add(split[1]);
                    }
                    lastHead = split[0];
                }
            }
        }

        @Override
        public void map(LongWritable rowNumber, Text value, Context context) throws IOException,  InterruptedException {
            String[] split = value.toString().split("\t");
            String word = split[0];

        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        int counter;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            counter = 0;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values){
//                counter++;
//                if(counter < 1000){
                String[] vals = value.toString().split("$");
                String word = vals[0];
                for(int i = 2; i<vals.length; i++){
                    String[] occFeat = vals[i].split(":");
                    for (int j = 1; j< occFeat.length; j++){
                        context.write(new Text(word+":"+vals[1]), new Text(occFeat[j]+":"+occFeat[0]));
                        //      dog:sum   bite-verb:70
                    }
                }
//                }
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

//        @Override
//        public void run(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            this.setup(context);
//
//            try {
//                while(context.nextKey() && counter < 1000) {
//                    this.reduce(context.getCurrentKey(), context.getValues(), context);
//                    Iterator<Text> iter = context.getValues().iterator();
//                    if (iter instanceof ReduceContext.ValueIterator) {
//                        ((ReduceContext.ValueIterator)iter).resetBackupStore();
//                    }
//                }
//            } finally {
//                this.cleanup(context);
//            }
//
//        }
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