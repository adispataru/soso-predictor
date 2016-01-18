package ro.ieat.soso.reasoning.controllers.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.persistence.CoalitionRepository;

/**
 * Created by adrian on 14.01.2016.
 */
@RestController
public class CoalitionController {
    @Autowired
    CoalitionRepository coalitionRepository;
    public static String coalitionTargetUrl = "http://localhost:8090/coalition";

    @RequestMapping(method = RequestMethod.POST, path = "/coalitions", consumes = "application/json")
    public Coalition updateCoalition(@RequestBody Coalition c){
        coalitionRepository.save(c);

        //send to coalition matcher
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        restTemplate.postForObject(coalitionTargetUrl, c, Coalition.class, headers);
        return c;

    }

    @RequestMapping(method = RequestMethod.GET, path = "/coalitions/size")
    public Long getCoalitionSize(){
        return coalitionRepository.count();
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/coalitions")
    public void deleteAll(){
        coalitionRepository.deleteAll();
    }

}
