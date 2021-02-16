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

public class Step4 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable line, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String word = split[0];
            String val = split[1];
            context.write(new Text(word), new Text(val));
            //alligator:sum     feat1:occ:totalocc
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, MapWritable> {
        MapWritable map;
        double wordSum;
        double featureSum;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            map = new MapWritable();
            double[] sums = loadSums();
            wordSum = sums[0];
            featureSum = sums[1];
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] splitKey = key.toString().split(":");
            String word = splitKey[0];
            for (Text value : values) {
                String[] vals = value.toString().split(":");
                Text feature = new Text(vals[0]);
                //eq5
                double occ5 = Double.parseDouble(vals[1]);
                //eq6
                double sum = Double.parseDouble(splitKey[1]);
                double totalOcc = Double.parseDouble(vals[2]);
                double occ6 = occ5 / sum;
                //eq7
                double plf = occ5 / wordSum;
                double pl = sum / wordSum;
                double pf = totalOcc / featureSum;
                double occ7 = (Math.log(plf / (pl * pf)) / Math.log(2));
                //eq8
                double rootplpf = Math.sqrt(pl * pf);
                double occ8 = (plf - (pl * pf)) / rootplpf;
                map.put(feature, new Text(occ5 + ":" + occ6 + ":" + occ7 + ":" + occ8));
            }
            context.write(new Text(word), map);
            map.clear();
            //
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

        private double[] loadSums() {
            double[] ret = new double[2];
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();
            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3").key("totals.txt").build(),
                    ResponseTransformer.toFile(Paths.get("totals.txt")));
            File file = new File("totals.txt");
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                int i = 0;
                while ((line = br.readLine()) != null) {
                    if (i == 0) {
                        ret[0] = Double.parseDouble(line);
                    } else if (i == 1) {
                        ret[1] = Double.parseDouble(line);
                    }
                    i++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return ret;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step4");
        job.setJarByClass(Step4.class);
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
