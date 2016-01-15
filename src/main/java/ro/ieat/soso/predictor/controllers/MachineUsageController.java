package ro.ieat.soso.predictor.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;

import javax.servlet.http.HttpServletRequest;

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

    @RequestMapping(method = RequestMethod.POST, path = "/coalitions/{id}/schedule", consumes = "application/json")
    public ResponseEntity<ScheduledJob> assignTaskUsageToMachine(@RequestBody ScheduledJob job, HttpServletRequest request){

        long jobId = job.getJobId();
        Job j = jobRepository.findOne(jobId);

        if(j != null) {

        String log = j.getLogicJobName();


//            if(CoalitionReasoner.appDurationMap.containsKey(log)){
//                job.setFinishTime(job.getTimeToStart() + CoalitionReasoner.appDurationMap.get(log).getMax());
//            }
            //instead set real time here
            job.setFinishTime(j.getFinishTime());

            for(Long taskId : job.getTaskMachineMapping().keySet()){
                long machineId = job.getTaskMachineMapping().get(taskId);
                Machine m = machineRepository.findOne(machineId);
                TaskUsage usage = taskUsageMappingRepository.findByJobIdAndTaskIndex(jobId, taskId);
                if(m != null) {
                    m.getTaskUsageList().add(usage.getId());
                    machineRepository.save(m);
                }
            }
//            machineRepository.assignedJobs.add(job.getJobId());
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
