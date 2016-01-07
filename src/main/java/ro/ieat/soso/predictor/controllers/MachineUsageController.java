package ro.ieat.soso.predictor.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by adrian on 07.01.2016.
 */
@RestController
public class MachineUsageController {

    @RequestMapping(method = RequestMethod.POST, path = "/machines/{id}/tasks", consumes = "text/plain")
    public ResponseEntity<Object> assignTaskUsageToMachine(@PathVariable("id") String id, @RequestBody String o, HttpServletRequest request){
        System.out.printf("Received: %s %s\n", id, o);

        return new ResponseEntity<Object>(o, HttpStatus.OK);
    }
}
