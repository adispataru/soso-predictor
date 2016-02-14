package ro.ieat.soso.reasoning.controllers;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.predictor.prediction.JobDuration;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by adrian on 10.01.2016.
 * CoalitionClient is responsible for communication with the consumer of coalitions, providing methods to reason
 * the JobMatcher with coalitions status.
 */
public class CoalitionClient{


    private static RestTemplate restTemplate;
    private static String coalitionTargetUrl = "http://localhost:8088/coalitions";
    private static final Logger LOG = Logger.getLogger("CoalitionClient");

    public void sendCoalition(Coalition c){
        restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        restTemplate.postForObject(coalitionTargetUrl, c, Coalition.class, headers);

    }

    public ScheduledJob sendJobRequest(Job j, String targetUrl){
        if(j.getJobId() != -1)
            LOG.info("Job to send:\n" + j.toString());
        restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ScheduledJob s = restTemplate.postForObject(targetUrl, j, ScheduledJob.class, headers);

        if(s != null)
            Logger.getLogger("JobRequester").info("event: " + s.getJobId());

        return s;
    }

    public void sendJobRuntimePrediction(List<JobDuration> jobs){
        restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        for(JobDuration jd : jobs) {
            restTemplate.postForEntity("http://localhost:8090/jobDuration", jd, JobDuration.class, headers);
            restTemplate.postForObject("http://localhost:8091/jobDuration", jd, JobDuration.class, headers);
        }
    }

    public void deleteCoalitionsFromRepository(){
        restTemplate = new RestTemplate();
        restTemplate.delete(coalitionTargetUrl);
    }
}
