package com.anand.emr.batchprocessor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {
    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job sparkWordCountJob;

    @PostMapping("/executeWordCountStep")
    public void executeWordCountStep() throws Exception {
        jobLauncher.run(sparkWordCountJob, new JobParameters());
    }

}
