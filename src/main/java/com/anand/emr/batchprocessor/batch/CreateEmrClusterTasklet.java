package com.anand.emr.batchprocessor.batch;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class CreateEmrClusterTasklet implements Tasklet {

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


        final StepFactory stepFactory = new StepFactory();
        final StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        final String logUri = "s3://aa-emr-logs";
        final Application spark = new Application().withName("Spark");
        final BootstrapActionConfig installBoto3 = new BootstrapActionConfig()
                .withName("Install Boto 3")
                .withScriptBootstrapAction(new ScriptBootstrapActionConfig().withPath("s3://aa-python-deps/install_boto3.sh"));

        final BootstrapActionConfig installSnowflakeJars = new BootstrapActionConfig()
                .withName("Install Snowflake Jars")
                .withScriptBootstrapAction(new ScriptBootstrapActionConfig().withPath("s3://aa-python-deps/install_jars.sh"));

        final BootstrapActionConfig setProperties = new BootstrapActionConfig()
                .withName("Set Properties")
                .withScriptBootstrapAction(new ScriptBootstrapActionConfig().withPath("s3://aa-python-deps/set_properties.sh"));

        final BootstrapActionConfig[] bootstrapActions = new BootstrapActionConfig[]{installBoto3, installSnowflakeJars, setProperties};

        final InstanceGroupConfig masterNodeConfig = new InstanceGroupConfig().withInstanceCount(1)
                .withInstanceRole("MASTER")
                .withName("Master nodes")
                .withInstanceType("m4.4xlarge")
                .withMarket(MarketType.ON_DEMAND)
                .withInstanceCount(2);

        final InstanceGroupConfig workerNodeConfig = new InstanceGroupConfig().withInstanceCount(1)
                .withInstanceRole("CORE")
                .withName("Slave nodes")
                .withInstanceType("m4.xlarge")
                .withMarket(MarketType.ON_DEMAND)
                .withInstanceCount(10);

        final InstanceGroupConfig[] instanceGroupConfigs = new InstanceGroupConfig[]{
                masterNodeConfig,
                workerNodeConfig
        };

        final RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("aa-emr-cluster-java")
                .withReleaseLabel("emr-5.30.1")
                //.withSteps(enableDebugging)
                .withApplications(spark)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withVisibleToAllUsers(true)
                .withInstances(
                        new JobFlowInstancesConfig()
                                .withEc2KeyName("emr-key-pair")
                                .withInstanceGroups(instanceGroupConfigs)
                                .withKeepJobFlowAliveWhenNoSteps(true)
                )
                .withBootstrapActions(bootstrapActions);

        final RunJobFlowResult result = emrClient.runJobFlow(request);
        System.out.println("Cluster ID : " + result.getJobFlowId());

        chunkContext.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getExecutionContext()
                .put("clusterId", result.getJobFlowId());

        return RepeatStatus.FINISHED;
    }
}
