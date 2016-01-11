package ro.ieat.soso.reasoning.controllers;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;

import java.util.logging.Logger;

/**
 * Created by adrian on 10.01.2016.
 * CoalitionClient is responsible for communication with the consumer of coalitions, providing methods to update
 * the JobMatcher with coalitions status.
 */
public class CoalitionClient {

    private static RestTemplate restTemplate;
    private static String coalitionTargetUrl = "http://localhost:8090/coalition";
    private static String jobRequestTargetUrl = "http://localhost:8090/job";

    public static void sendCoalition(Coalition c){
        restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        restTemplate.postForObject(coalitionTargetUrl, c, Coalition.class, headers);

    }

    public static void sendJobRequest(Job j){
        restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ScheduledJob s = restTemplate.postForObject(jobRequestTargetUrl, j, ScheduledJob.class, headers);

        if(s != null)
            Logger.getLogger("Dummy").info("event: " + s.getJobId());
    }
}
