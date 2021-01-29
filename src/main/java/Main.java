import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;


import java.nio.file.Path;

public class Main {
    private static final AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());


    public static void main(String... args) {

//        uploadJars();

        //todo: create input string with for, from 99 input files
        String[] biarcs = new String[4];
        biarcs[0] = "s3://dsp-211-ass3/step1";
        biarcs[1] = "s3://dsp-211-ass3/v1/part-r-00000";
        biarcs[2] = "s3://assignment3dsp/biarcs/biarcs.21-of-99";
        biarcs[3] = "s3://assignment3dsp/biarcs/biarcs.22-of-99";
//        for (int i=1; i< biarcs.length; i++){
//            biarcs[i] = "biarcs.21-of-99.gz";
//        }


        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        //STEP0
        HadoopJarStepConfig step0 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step0.jar")
                .withArgs("s3://dsp-211-ass3/word-relatedness.txt","s3://dsp-211-ass3/v1");
        StepConfig stepConfig0 = new StepConfig()
                .withName("test")
                .withHadoopJarStep(step0)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step1.jar")
                .withArgs(biarcs);
        StepConfig stepConfig1 = new StepConfig()
                .withName("test")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("dspass1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("semanticSimilarity")
                .withInstances(instances)
                .withSteps(stepConfig1)
                .withLogUri("s3n://dsp-211-ass3/logs/")
                .withServiceRole("EMR_Role")
                .withJobFlowRole("EMR_EC2_Role")
                .withReleaseLabel("emr-5.32.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }

    private static void uploadJars() {
        try {
            String key = "Step1.jar";
            Region region = Region.US_EAST_1;
            S3Client s3 = S3Client.builder().region(region).build();

//            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket("dsp-211-ass3").key(key).build();
//            s3.deleteObject(deleteObjectRequest);

            s3.putObject(PutObjectRequest.builder()
                            .bucket("dsp-211-ass3")
                            .key(key).acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get(key));


        } catch (S3Exception e) {
            e.printStackTrace();
        }
    }

//    public static void printFile(){
//        File file = new File("/Users/awadi/Desktop/hadoop logs/biarcs.21-of-99.gz");
//        GZIPInputStream in = null;
//        try {
//            in = new GZIPInputStream(new FileInputStream(file));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        Reader decoder = new InputStreamReader(in);
//        BufferedReader br = new BufferedReader(decoder);
//
//        String line;
//        int lines = 0;
//        while (true) {
//            try {
//                if (!((line = br.readLine()) != null)) break;
//                System.out.println(line);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            lines++;
//        }
//    }
}