import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.evaluation.Prediction;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils;
import weka.core.matrix.Matrix;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Weka {
    public static void main(String[] args) {
        try {
            //download file from s3
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();
            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3").key("step6out/part-r-00000").build(),
                    ResponseTransformer.toFile(Paths.get("train.arff")));

            //train
            ConverterUtils.DataSource source = new ConverterUtils.DataSource("train.arff");
            Instances data = source.getDataSet();
            if (data.classIndex() == -1) {
                data.setClassIndex(data.numAttributes() - 1);
            }
            J48 tree = new J48();
            tree.buildClassifier(data);

            Evaluation eval = new Evaluation(data);
            eval.crossValidateModel(tree, data, 10, new Random(1));

            System.out.println(eval.toClassDetailsString());
            System.out.println(tree.toString());
            System.out.println(eval.toMatrixString());

            //test
//            ConverterUtils.DataSource s2 = new ConverterUtils.DataSource("test1.arff");
//            Instances data2 = s2.getDataSet();
//            data2.setClassIndex(data2.numAttributes() - 1);
//            for(int i=0;i<data2.size();i++) {
//                Instance j = data2.instance(i);
//                double classifyInstance=tree.classifyInstance(j);
//                if(classifyInstance!=1) {
//                    System.out.println(j.toString()+" " +i + " " + classifyInstance);
//                }
//            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
