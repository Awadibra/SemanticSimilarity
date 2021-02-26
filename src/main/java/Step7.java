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

public class Step7 {
    public static void main(String[] args) {
        try {
            //download file from s3
//            Region region = Region.US_EAST_1;
//            S3Client s3 = S3Client.builder().region(region).build();
//            s3.getObject(GetObjectRequest.builder().bucket("dsp-211-ass3/step6out").key("part-r-00000").build(),
//                    ResponseTransformer.toFile(Paths.get("data.arff")));


            ConverterUtils.DataSource source = new ConverterUtils.DataSource("step6output.arff");
            Instances data = source.getDataSet();
            if (data.classIndex() == -1) {
                data.setClassIndex(data.numAttributes() - 1);
            }
            J48 tree = new J48();
            tree.buildClassifier(data);



//            Instance i2=data.instance(15);

//            System.out.println(i1.toString());
//            System.out.println(i1.classValue());
//            System.out.println(i1.toString(i1.numAttributes() - 1));

//            System.out.println(tree.);
//            System.out.println(tree.classifyInstance(i2));

            Evaluation eval = new Evaluation(data);
            eval.crossValidateModel(tree, data, 10, new Random(1));


            int count=0;
            for(int i=0;i<data.size();i++) {
                Instance j = data.instance(i);
                double classifyInstance=tree.classifyInstance(j);
//                if(classifyInstance!=1) {
                    System.out.println(i + " " + classifyInstance+" "+j.toString());
                    count++;
//                }
            }
//            System.out.println(count);
            ConverterUtils.DataSource s2 = new ConverterUtils.DataSource("try.arff");
            Instances data2 = source.getDataSet();

                data2.setClassIndex(data2.numAttributes() - 1);

            Instance curr=data2.get(0);
            System.out.println(tree.classifyInstance(curr));

//
//            ArrayList<Prediction> predictions=eval.predictions();
//            for(Prediction p:predictions){
//                System.out.println(p.actual()+" "+p.predicted()+" ");
//            }
//            double[][] matrix=eval.confusionMatrix();
//            System.out.println(eval.toMatrixString());
//            todo: f1-score
//            System.out.println(eval.correct());
//            System.out.println(eval.errorRate());
//            System.out.println(eval.toClassDetailsString());
////
////
//            System.out.println(tree.toSummaryString());
//            System.out.println(tree.toString());

//            System.out.println(eval.get);
//            System.out.println(eval.precision(data.classIndex()));
//            System.out.println(eval.recall(data.classIndex()));

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
