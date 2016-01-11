package ro.ieat.soso.predictor.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.predictor.persistence.MachineRepository;
import ro.ieat.soso.reasoning.CoalitionReasoner;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by adrian on 07.01.2016.
 * Used to assign task usage to machines.
 */
@RestController
public class MachineUsageController {

    @RequestMapping(method = RequestMethod.POST, path = "/coalitions/{id}/schedule", consumes = "application/json")
    public ResponseEntity<ScheduledJob> assignTaskUsageToMachine(@RequestBody ScheduledJob job, HttpServletRequest request){

        long jobId = job.getJobId();
        String log = MachineRepository.jobRepo.get(jobId).getLogicJobName();

        if(MachineRepository.jobRepo.containsKey(jobId)) {
            if(CoalitionReasoner.appDurationMap.containsKey(log)){
                job.setFinishTime(job.getSubmitTime() + CoalitionReasoner.appDurationMap.get(log).getMax());
            }
            for(TaskHistory taskHistory : MachineRepository.jobRepo.get(jobId).getTaskHistory().values()){
                long taskId = taskHistory.getTaskIndex();
                long machineId = job.getTaskMachineMapping().get(taskId);
                Machine m = MachineRepository.findOne(machineId);
                if(m != null)
                    m.getTaskUsageList().add(taskHistory.getTaskUsage());
                else{
                    System.out.printf("Machine not found!: %d", machineId);
                }
            }
            MachineRepository.assignedJobs.add(job.getJobId());
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.OK);
        } else{
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.NOT_FOUND);
        }

    }

    @RequestMapping(method = RequestMethod.POST, path = "assign/usage")
    public ResponseEntity<String> assignRestOfJobs(){

        for(Job j : MachineRepository.jobRepo.values()){
            if(MachineRepository.assignedJobs.contains(j.getJobId()))
                continue;

            for(TaskHistory taskHistory : j.getTaskHistory().values()){
                long taskId = taskHistory.getTaskIndex();
                long machineId = taskHistory.getMachineId();
                Machine m = MachineRepository.findOne(machineId);
                if(m != null)
                    m.getTaskUsageList().add(taskHistory.getTaskUsage());
                else{
                    System.out.printf("Machine not found!: %d", machineId);
                }
            }
            MachineRepository.assignedJobs.add(j.getJobId());

        }


        return new ResponseEntity<String>("ok", HttpStatus.OK);


    }

    @RequestMapping(method =  RequestMethod.POST, path = "assign/normalJob/{id}")
    public ResponseEntity<String> assignJob(@PathVariable("id") long id){
        Job j = MachineRepository.jobRepo.get(id);
        if(MachineRepository.assignedJobs.contains(j.getJobId()))
            return new ResponseEntity<String>("ok", HttpStatus.NO_CONTENT);;

        for(TaskHistory taskHistory : j.getTaskHistory().values()){
            long taskId = taskHistory.getTaskIndex();
            long machineId = taskHistory.getMachineId();
            Machine m = MachineRepository.findOne(machineId);
            if(m != null)
                m.getTaskUsageList().add(taskHistory.getTaskUsage());
            else{
                System.out.printf("Machine not found!: %d", machineId);
            }
        }
        MachineRepository.assignedJobs.add(j.getJobId());

        return new ResponseEntity<String>("ok", HttpStatus.OK);
    }


}
