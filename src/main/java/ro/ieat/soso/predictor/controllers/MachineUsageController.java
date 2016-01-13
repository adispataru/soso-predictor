package ro.ieat.soso.predictor.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.predictor.persistence.MachineRepository;

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
        MachineRepository machineRepository = MachineRepository.getInstance();
        String log = machineRepository.jobRepo.get(jobId).getLogicJobName();

        if(machineRepository.jobRepo.containsKey(jobId)) {
//            if(CoalitionReasoner.appDurationMap.containsKey(log)){
//                job.setFinishTime(job.getTimeToStart() + CoalitionReasoner.appDurationMap.get(log).getMax());
//            }
            //instead set real time here
            job.setFinishTime(machineRepository.jobRepo.get(jobId).getFinishTime());

            for(TaskHistory taskHistory : machineRepository.jobRepo.get(jobId).getTaskHistory().values()){
                long taskId = taskHistory.getTaskIndex();
                long machineId = job.getTaskMachineMapping().get(taskId);
                System.out.printf("Task: %s from %d", taskHistory, jobId);
                taskHistory.getTaskUsage().setTaskIndex(taskId);
                Machine m = machineRepository.findOne(machineId);
                if(m != null)
                    m.getTaskUsageList().add(taskHistory.getTaskUsage());
                else{
                    System.out.printf("Machine not found!: %d", machineId);
                }
            }
            machineRepository.assignedJobs.add(job.getJobId());
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.OK);
        } else{
            return new ResponseEntity<ScheduledJob>(job, HttpStatus.NOT_FOUND);
        }

    }

    @RequestMapping(method = RequestMethod.POST, path = "assign/usage")
    public ResponseEntity<String> assignRestOfJobs(){

        MachineRepository machineRepository = MachineRepository.getInstance();
        for(Job j : machineRepository.jobRepo.values()){
            if(machineRepository.assignedJobs.contains(j.getJobId()))
                continue;

            for(TaskHistory taskHistory : j.getTaskHistory().values()){
                long taskId = taskHistory.getTaskIndex();
                long machineId = taskHistory.getMachineId();
                Machine m = machineRepository.findOne(machineId);
                if(m != null)
                    m.getTaskUsageList().add(taskHistory.getTaskUsage());
                else{
                    System.out.printf("Machine not found!: %d", machineId);
                }
            }
            machineRepository.assignedJobs.add(j.getJobId());

        }


        return new ResponseEntity<String>("ok", HttpStatus.OK);


    }

    @RequestMapping(method =  RequestMethod.POST, path = "assign/normalJob/{id}")
    public ResponseEntity<String> assignJob(@PathVariable("id") long id){
        MachineRepository machineRepository = MachineRepository.getInstance();
        Job j = machineRepository.jobRepo.get(id);
        if(machineRepository.assignedJobs.contains(j.getJobId()))
            return new ResponseEntity<String>("ok", HttpStatus.NO_CONTENT);;

        for(TaskHistory taskHistory : j.getTaskHistory().values()){
            long taskId = taskHistory.getTaskIndex();
            long machineId = taskHistory.getMachineId();
            Machine m = machineRepository.findOne(machineId);
            if(m != null)
                m.getTaskUsageList().add(taskHistory.getTaskUsage());
            else{
                System.out.printf("Machine not found!: %d", machineId);
            }
        }
        machineRepository.assignedJobs.add(j.getJobId());

        return new ResponseEntity<String>("ok", HttpStatus.OK);
    }


}
