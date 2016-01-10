package ro.ieat.soso.reasoning.controllers;

import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;

/**
 * Created by adrian on 10.01.2016.
 * CoalitionClient is responsible for communication with the consumer of coalitions, providing methods to update
 * the JobMatcher with coalitions status.
 */
public class CoalitionClient {

    private static RestTemplate restTemplate;
    private static String targetUrl;

    public static void sendCoalition(Coalition c){
        restTemplate = new RestTemplate();
        restTemplate.put(targetUrl, c);

    }
}
