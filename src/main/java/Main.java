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
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        //STEP0 - build v1
        HadoopJarStepConfig step0 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step0.jar")
                .withArgs("s3://dsp-211-ass3/word-relatedness.txt","s3://dsp-211-ass3/v1");
        StepConfig stepConfig0 = new StepConfig()
                .withName("test")
                .withHadoopJarStep(step0)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //STEP1 - get words from corpus which also in v1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step1.jar")
                .withArgs("s3://assignment3dsp/biarcs/biarcs.13-of-99","s3://dsp-211-ass3/step1out");
//                .withArgs("s3://assignment3dsp/biarcs","s3://dsp-211-ass3/step1out");
        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step2.jar")
                .withArgs("s3://dsp-211-ass3/step1out","s3://dsp-211-ass3/step2out");
        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step3.jar")
                .withArgs("s3://dsp-211-ass3/step2out","s3://dsp-211-ass3/step3out");
        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP4
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step4.jar")
                .withArgs("s3://dsp-211-ass3/step3out","s3://dsp-211-ass3/step4out");
        StepConfig stepConfig4 = new StepConfig()
                .withName("step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //STEP5
        HadoopJarStepConfig step5 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step5.jar")
                .withArgs("s3://dsp-211-ass3/step4out","s3://dsp-211-ass3/step5out");
        StepConfig stepConfig5 = new StepConfig()
                .withName("step5")
                .withHadoopJarStep(step5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Step6
        HadoopJarStepConfig step6 = new HadoopJarStepConfig()
                .withJar("s3://dsp-211-ass3/Step6.jar")
                .withArgs("s3://dsp-211-ass3/step5out","s3://dsp-211-ass3/step6out");
        StepConfig stepConfig6 = new StepConfig()
                .withName("step6")
                .withHadoopJarStep(step6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType(InstanceType.C5_XLARGE.toString())
                .withSlaveInstanceType(InstanceType.C5_XLARGE.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("dspass1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("semanticSimilarity")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3,stepConfig4,stepConfig5,stepConfig6)
                .withLogUri("s3n://dsp-211-ass3/logs/")
                .withServiceRole("EMR_Role")
                .withJobFlowRole("EMR_EC2_Role")
                .withReleaseLabel("emr-5.32.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}