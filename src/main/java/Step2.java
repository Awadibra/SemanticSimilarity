import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;

public class Step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String> map;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();
            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3").key("sums.txt").build(),
                    ResponseTransformer.toFile(Paths.get("sums.txt")));
            map = new HashMap<>();
            File file = new File("sums.txt");
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] split = line.split("\t");
                    String word = split[0];
                    String sum = split[1];
                    map.put(word, sum);
                }
            }
        }

        @Override
        public void map(LongWritable rowNumber, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String word = split[0];
            String[] occFeat = split[1].split(":");
            String occ = occFeat[0];
            String sum = map.get(word);
            for (int j = 1; j < occFeat.length; j++) {
                String feature = occFeat[j];
                context.write(new Text(word + ":" + sum), new Text(feature + ":" + occ));
                //      dog:sum   bite-verb:70
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, MapWritable> {
        MapWritable map;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            map = new MapWritable();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] vals = value.toString().split(":");
                Text feature = new Text(vals[0]);
                long occVal = Long.parseLong(vals[1]);
                LongWritable occ = new LongWritable(occVal);
                if (map.containsKey(feature)) {
                    LongWritable temp = (LongWritable) map.get(feature);
                    long newVal = occVal + temp.get();
                    map.put(feature, new LongWritable(newVal));
                } else {
                    map.put(feature, occ);
                }
            }
            context.write(key, map);
            map.clear();
            // alligator:sum    map{feature:occ,...}
            //alligator:55      map{bite-sub:10}

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
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