package ro.ieat.soso.predictor.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.evaluator.TestTaskUsage;
import ro.ieat.soso.persistence.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

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
    @Autowired
    TestTaskRepository testTaskRepository;

    @RequestMapping(method = RequestMethod.POST, path = "/coalitions/{id}/schedule", consumes = "application/json")
    public ResponseEntity<ScheduledJob> assignTaskUsageToMachine(@PathVariable long id, @RequestBody ScheduledJob job, HttpServletRequest request){

        Coalition coalition = coalitionRepository.findOne(id);
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.findByJobId(job.getJobId());
        long testid = testTaskRepository.count();
        for(TaskUsage taskUsage : taskUsageList){
            TestTaskUsage t = new TestTaskUsage();
            t.endTime = taskUsage.getFinishTime();
            t.startTime = taskUsage.getStartTime();
            t.jobId = job.getJobId();
            t.logicJobname = taskUsage.getLogicJobName();
            t.machineId = job.getTaskMachineMapping().get(taskUsage.getTaskIndex());
            t.coalitionId = id;
            t.taskUsageId = taskUsage.getId();
            testTaskRepository.save(t);
        }


        long jobId = job.getJobId();
        Job j = jobRepository.findOne(jobId);
        if(j != null) {

            job.setFinishTime(j.getFinishTime());
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.OK);
        } else{
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.NOT_FOUND);
        }

    }

//    @RequestMapping(method = RequestMethod.POST, path = "assign/usage")
//    public ResponseEntity<String> assignRestOfJobs(){
//
//
//        for(Job j : machineRepository.jobRepo.values()){
//            if(machineRepository.assignedJobs.contains(j.getJobId()))
//                continue;
//
//            for(TaskHistory taskHistory : j.getTaskHistory().values()){
//                long taskId = taskHistory.getTaskIndex();
//                long machineId = taskHistory.getMachineId();
//                Machine m = machineRepository.findOne(machineId);
//                if(m != null)
//                    m.getTaskUsageList().add(taskHistory.getTaskUsage());
//                else{
//                    System.out.printf("Machine not found!: %d", machineId);
//                }
//            }
//            machineRepository.assignedJobs.add(j.getJobId());
//
//        }
//
//
//        return new ResponseEntity<String>("ok", HttpStatus.OK);
//
//
//    }

    @RequestMapping(method =  RequestMethod.POST, path = "/assign/normalJob/{id}")
    public ResponseEntity<String> assignJob(@PathVariable("id") long id){

        Job j = jobRepository.findOne(id);

//        for(TaskHistory taskHistory : j.getTaskHistory().values()){
//            long taskId = taskHistory.getTaskIndex();
//            long machineId = taskHistory.getMachineId();
//            Machine m = machineRepository.findOne(machineId);
//            if(m != null) {
//                m.getTaskUsageList().add(taskHistory.getTaskUsage());
//                machineRepository.save(m);
//            }
//            else{
//                System.out.printf("Machine not found!: %d", machineId);
//            }
//        }


        return new ResponseEntity<String>("ok", HttpStatus.OK);
    }

    @RequestMapping(method =  RequestMethod.POST, path = "/assign/usage/{id}/{usageId}", consumes = "application/json")
    public ResponseEntity<Long> assignUsage(@PathVariable("id") long id, @PathVariable("usageId") Long usage){

            Machine m = machineRepository.findOne(id);
            if(m != null) {
                m.getTaskUsageList().add(usage);
                machineRepository.save(m);
            }
            else{
                System.out.printf("Machine not found!: %d", id);
            }

        return new ResponseEntity<Long>(usage, HttpStatus.OK);
    }


}
