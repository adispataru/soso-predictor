package ro.ieat.soso.reasoning.controllers.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.persistence.JobRepository;

/**
 * Created by adrian on 14.01.2016.
 */
@RestController
public class TaskUsageController {
    @Autowired
    JobRepository coalitionRepository;
    private RestTemplate restTemplate;

    @RequestMapping(method = RequestMethod.POST, path = "/tasks", consumes = "application/json")
    public Job updateCoalition(@RequestBody Job c){
        coalitionRepository.save(c);

        return c;

    }

    @RequestMapping(method = RequestMethod.GET, path = "/tasks/size")
    public Long getCoalitionSize(){
        return coalitionRepository.count();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/tasks/{id}")
    public Job getMachine(@PathVariable Long id){
        return coalitionRepository.findOne(id);
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/tasks")
    public void deleteAll(){
        coalitionRepository.deleteAll();
    }


}
