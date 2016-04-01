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
    private static final Logger LOG = Logger.getLogger("CoalitionClient");
    public static String[] targetUrls = {"http://localhost:8090/",
            "http://localhost:8091/", "http://localhost:8092/"};

    public void sendCoalition(Coalition c){

        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        for(String coalitionTargetUrl : targetUrls) {
            restTemplate.postForObject(coalitionTargetUrl + "coalition", c, Coalition.class, headers);
        }

    }

    public void sendCoalitionToComponent(Coalition c, int componentIndex){
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        restTemplate.postForObject(targetUrls[componentIndex] + "coalition", c, Coalition.class, headers);

    }
    public void deleteCoalitionFromComponent(Coalition c, int componentIndex){
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        restTemplate.delete(targetUrls[componentIndex] + "/DELETE/coalitions/" + c.getId());

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
            for(String url : targetUrls) {
                restTemplate.postForEntity(url + "jobDuration", jd, Object.class, headers);
            }
        }
    }

    public void deleteCoalitionsFromRepository(){

    }
}
