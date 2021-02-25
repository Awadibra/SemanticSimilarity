import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils;
import weka.core.matrix.Matrix;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

public class Step7 {
    public static void main(String[] args) {
        try {
            //download file from s3
//            Region region = Region.US_EAST_1;
//            S3Client s3 = S3Client.builder().region(region).build();
//            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3/step6out").key("part-r-00000").build(),
//                    ResponseTransformer.toFile(Paths.get("data.arff")));


            ConverterUtils.DataSource source = new ConverterUtils.DataSource("data.arff");
            Instances data = source.getDataSet();
            if (data.classIndex() == -1) {
                data.setClassIndex(data.numAttributes() - 1);
            }
            J48 tree = new J48();
            Evaluation eval = new Evaluation(data);
            eval.crossValidateModel(tree, data, 10, new Random(1));

//            System.out.println(eval.precision(data.classIndex()));
//            System.out.println(eval.recall(data.classIndex()));
//            todo: f1-score
            System.out.println(eval.toClassDetailsString());
            System.out.println(eval.falseNegativeRate(data.classIndex()));
            System.out.println(eval.trueNegativeRate(data.classIndex()));
            System.out.println(eval.falsePositiveRate(data.classIndex()));
//            System.out.println(eval.truePositiveRate(data.classIndex()));
//            System.out.println(eval.recall(data.classIndex()));
            System.out.println(eval.correct());
            System.out.println(eval.errorRate());

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
