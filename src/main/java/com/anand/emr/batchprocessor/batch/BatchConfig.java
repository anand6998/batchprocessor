package com.anand.emr.batchprocessor.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfig extends DefaultBatchConfigurer {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public CreateEmrClusterTasklet createEmrClusterTasklet() {
        return new CreateEmrClusterTasklet();
    }

    @Bean
    public WaitForClusterStartTasklet waitForClusterStartTasklet() {
        return new WaitForClusterStartTasklet();
    }

    @Bean
    public SubmitWordCountTaskTasklet submitWordCountTaskTasklet() {
        return new SubmitWordCountTaskTasklet();
    }

    @Bean
    public WaitForStepCompletionTasklet waitForStepCompletionTasklet() {
        return new WaitForStepCompletionTasklet();
    }


    @Bean
    public TerminateClusterTasklet terminateClusterTasklet() {
        return new TerminateClusterTasklet();
    }


    @Bean
    public Step createEmrClusterStep() {
        return stepBuilderFactory.get("createEmrClusterStep")
                .tasklet(createEmrClusterTasklet())
                .build();
    }

    @Bean
    Step waitForClusterStartStep() {
        return stepBuilderFactory.get("waitForClusterStartStep")
                .tasklet(waitForClusterStartTasklet())
                .build();
    }

    @Bean
    public Step submitWordCountTaskStep() {
        return stepBuilderFactory.get("submitWordCountTaskJob")
                .tasklet(submitWordCountTaskTasklet())
                .build();
    }

    @Bean
    public Step waitForStepCompletionStep() {
        return stepBuilderFactory.get("waitForStepCompletion")
                .tasklet(waitForStepCompletionTasklet())
                .build();
    }

    @Bean
    public Step terminateClusterStep() {
        return stepBuilderFactory.get("terminateClusterStep")
                .tasklet(terminateClusterTasklet())
                .build();
    }

    @Bean
    public Job runSparkWordCountJob() {
        return jobBuilderFactory.get("runSparkWordCountJob")
                .start(createEmrClusterStep())
                .next(waitForClusterStartStep())
                .next(submitWordCountTaskStep())
                .next(waitForStepCompletionStep())
                .next(terminateClusterStep())
//                .next(concatOutputFilesStep())
//                .next(bcpFileToDb())
                .build();
    }
}
