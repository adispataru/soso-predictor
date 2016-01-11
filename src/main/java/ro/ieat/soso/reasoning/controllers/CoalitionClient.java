package ro.ieat.soso.reasoning.controllers;

import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.jobs.Job;

/**
 * Created by adrian on 10.01.2016.
 * CoalitionClient is responsible for communication with the consumer of coalitions, providing methods to update
 * the JobMatcher with coalitions status.
 */
public class CoalitionClient {

    private static RestTemplate restTemplate;
    private static String coalitionTargetUrl;
    private static String jobRequestTargetUrl;

    public static void sendCoalition(Coalition c){
        restTemplate = new RestTemplate();
        restTemplate.put(coalitionTargetUrl, c);

    }

    public static void sendJobRequest(Job j){
        restTemplate = new RestTemplate();
        restTemplate.postForEntity(jobRequestTargetUrl, j, Object.class);
    }
}
