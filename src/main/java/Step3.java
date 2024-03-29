import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Step3 {

    public static class MapperClass extends Mapper<Text, MapWritable, Text, Text> {


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            for (Map.Entry<Writable, Writable> in : value.entrySet()) {
                Text feature = new Text(in.getKey().toString());
                LongWritable occ = (LongWritable) in.getValue();
                context.write(feature, new Text(key.toString() + ":" + occ.get()));
                //feat1     alligator:sum:occ
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        HashMap<String, String> map;
        long totalOcc;
        long wordSum;
        long featureSum;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            map = new HashMap<>();
            totalOcc = 0;
            wordSum = 0;
            featureSum = 0;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            totalOcc = 0;
            for (Text value : values) {
                String[] split = value.toString().split(":");
                String word = split[0];
                String sum = split[1];
                String occ = split[2];
                wordSum += Long.parseLong(sum);
                totalOcc += Long.parseLong(occ);
                map.put(word + ":" + sum, occ);
            }
            featureSum+=totalOcc;
            for (Map.Entry<String, String> in : map.entrySet()) {
                String word = in.getKey();
                String occ = in.getValue();
                context.write(new Text(word), new Text(key.toString() + ":" + occ + ":" + totalOcc));
                //alligator:sum     feat1:occ:totalocc
            }
            map.clear();
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            uploadTotals(wordSum, featureSum);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void uploadTotals(long totalSum, long totalFeat) {
        try {
            PrintWriter writer = new PrintWriter("totals.txt", "UTF-8");
            writer.println(totalSum);
            writer.println(totalFeat);
            writer.close();
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();

//            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket("dsp-211-ass3").key(key).build();
//            s3.deleteObject(deleteObjectRequest);

            s3.putObject(PutObjectRequest.builder()
                            .bucket("dsp-211-ass3")
                            .key("totals.txt").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("totals.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
