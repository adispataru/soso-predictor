package ro.ieat.soso.reasoning.controllers.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.persistence.JobRepository;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by adrian on 14.01.2016.
 */
@RestController
public class JobController {
    @Autowired
    JobRepository coalitionRepository;
    private RestTemplate restTemplate;

    @RequestMapping(method = RequestMethod.POST, path = "/jobs", consumes = "application/json")
    public Job updateCoalition(@RequestBody Job c){
        coalitionRepository.save(c);

        return c;

    }

    @RequestMapping(method = RequestMethod.GET, path = "/jobs/size")
    public Long getCoalitionSize(){
        return coalitionRepository.count();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/jobs/{id}")
    public Job getMachine(@PathVariable Long id){
        return coalitionRepository.findOne(id);
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/jobs")
    public void deleteAll(){
        coalitionRepository.deleteAll();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/jobs/logic/{name}/{end}")
    public List<Job> getMachine(@PathVariable String name, @PathVariable Long end){
        List<Job> result = coalitionRepository.findByLogicJobName(name).stream()
                .filter(j -> j.getFinishTime() < end).collect(Collectors.toList());;

        return result;
    }

}
