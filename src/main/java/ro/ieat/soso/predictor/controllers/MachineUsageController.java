package ro.ieat.soso.predictor.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.persistence.CoalitionRepository;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by adrian on 07.01.2016.
 * Used to assign task usage to machines.
 */
@RestController
public class MachineUsageController {
    @Autowired
    MachineRepository machineRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    CoalitionRepository coalitionRepository;

    @RequestMapping(method = RequestMethod.POST, path = "/coalitions/{id}/schedule", consumes = "application/json")
    public ResponseEntity<ScheduledJob> assignTaskUsageToMachine(@PathVariable long id, @RequestBody ScheduledJob job, HttpServletRequest request){

        Logger LOG = Logger.getLogger("MachineUsageController");
        //Coalition coalition = coalitionRepository.findOne(id);
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.findByJobId(job.getJobId());

        for(TaskUsage taskUsage : taskUsageList){
            taskUsage.setAssignedMachineId(job.getTaskMachineMapping().get(taskUsage.getTaskIndex()));
        }
        taskUsageMappingRepository.save(taskUsageList);
        LOG.info("Scheduling " + job.getJobId());

        long jobId = job.getJobId();
        Job j = jobRepository.findOne(jobId);
        if(j != null) {

            job.setFinishTime(j.getFinishTime());
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.OK);
        } else{
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.NOT_FOUND);
        }

    }

}
