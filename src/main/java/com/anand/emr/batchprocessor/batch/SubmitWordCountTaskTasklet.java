package com.anand.emr.batchprocessor.batch;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class SubmitWordCountTaskTasklet implements Tasklet {
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        AWSCredentials awsCredentials = null;
        try {
            awsCredentials = new ProfileCredentialsProvider("chicago_crimes").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and that the profile name is defined within it.",
                    e);
        }


        final AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(Regions.US_EAST_1)
                .build();

        final String clusterId =
                chunkContext.getStepContext()
                        .getStepExecution()
                        .getJobExecution()
                        .getExecutionContext()
                        .getString("clusterId");
        /*
        spark-submit --deploy-mode cluster
        --master yarn --jars
        /home/hadoop/extraJars/snowflake-jdbc-3.12.11.jar,/home/hadoop/extraJars/spark-snowflake_2.11-2.8.1-spark_2.4.jar
        s3://aa-spark-scripts/spark_snowflake.py
        /home/hadoop/properties.txt
         */

        final String[] args = new String[] {
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--jars",
                "/home/hadoop/extraJars/snowflake-jdbc-3.12.11.jar,/home/hadoop/extraJars/spark-snowflake_2.11-2.8.1-spark_2.4.jar",
                "--executor-memory",
                "5g",
                "--driver-memory",
                "10g",
                "s3://aa-spark-scripts/word_count_aws.py"
        };

        final HadoopJarStepConfig wordCountConfig =
                new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs(args);

        final StepConfig wordCountJarStep = new StepConfig("RunWordCountStep", wordCountConfig)
                .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT);

        final AddJobFlowStepsResult jobFlowResult = emrClient.addJobFlowSteps(
                new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(wordCountJarStep)
        );

        System.out.println("StepIds: " + jobFlowResult.getStepIds().toString());
        final String stepId = jobFlowResult.getStepIds().get(0);
        chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .put("stepId", stepId);

        return RepeatStatus.FINISHED;
    }
}
