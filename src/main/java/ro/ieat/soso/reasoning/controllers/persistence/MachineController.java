package ro.ieat.soso.reasoning.controllers.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.persistence.MachineRepository;

/**
 * Created by adrian on 14.01.2016.
 */
@RestController
public class MachineController {
    @Autowired
    MachineRepository coalitionRepository;
    private RestTemplate restTemplate;
    private static String coalitionTargetUrl = "http://localhost:8090/coalition";

    @RequestMapping(method = RequestMethod.POST, path = "/machines", consumes = "application/json")
    public Machine updateCoalition(@RequestBody Machine c){
        coalitionRepository.save(c);

        return c;

    }

    @RequestMapping(method = RequestMethod.GET, path = "/machines/size")
    public Long getCoalitionSize(){
        return coalitionRepository.count();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/machines/{id}")
    public Machine getMachine(@PathVariable Long id){
        return coalitionRepository.findOne(id);
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/machines")
    public void deleteAll(){
        coalitionRepository.deleteAll();
    }

}
