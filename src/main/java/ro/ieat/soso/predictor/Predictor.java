package ro.ieat.soso.predictor;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.Duration;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.predictor.prediction.PredictionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by adrian on 11.12.2015.
 */

@RestController
public  class Predictor {

    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    MachineRepository machineRepository;

    @RequestMapping(method = RequestMethod.PUT, path = "/predict/allUsage/{historyStart}/{historyEnd}", consumes = "application/json")
    public void predictAllMachines(@PathVariable long historyStart,@PathVariable long historyEnd){
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndFinishTimeLessThan(historyStart, historyEnd);
        for(Machine m : machineRepository.findAll()){
            List<TaskUsage> usageList = taskUsageList.stream().filter(t -> t.getMachineId().equals(m.getId())).collect(
                    Collectors.toList()
            );
            List<TaskUsage> queue = new LinkedList<>();
            int processed = 1;
            List<TaskUsage> predictedTaskUsageList = new ArrayList<>();
            int lastIndex = 1;
            TaskUsage lastTaskUsage = taskUsageList.get(0);
            queue.add(lastTaskUsage);
            while(processed < taskUsageList.size()){
                TaskUsage taskUsage = taskUsageList.get(lastIndex);
                if(taskUsage.getJobId() == lastTaskUsage.getJobId() &&
                        taskUsage.getTaskIndex() == lastTaskUsage.getTaskIndex()){
                    queue.add(taskUsage);

                }else{
                    if(!queue.isEmpty()){
                        TaskUsage predicted = (TaskUsage) PredictionFactory.getPredictionMethod("taskUsage").predict(queue);
                        predictedTaskUsageList.add(predicted);
                        queue.clear();
                    }
                    queue.add(taskUsage);
                }
                processed++;
                lastIndex++;
            }
            TaskUsage machineUsage = new TaskUsage();
            for(TaskUsage tu : predictedTaskUsageList){
                if(machineUsage.getEndTime() < tu.getEndTime())
                    machineUsage.setEndTime(tu.getEndTime());
                if(machineUsage.getStartTime() < tu.getStartTime())
                    machineUsage.setStartTime(tu.getStartTime());
                machineUsage.addTaskUsage(tu);
            }


            m.setUsagePrediction(machineUsage);
            machineRepository.save(m);
        }

    }



    @RequestMapping(method = RequestMethod.POST, path = "/predict/usage/{historyStart}/{historyEnd}", consumes = "application/json")
    public TaskUsage predictMachineUsage(@RequestBody Machine m, @PathVariable long historyStart,
                                                 @PathVariable long historyEnd) throws IOException, InterruptedException {


        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndFinishTimeLessThan(historyStart, historyEnd);
        List<TaskUsage> usageList = taskUsageList.stream().filter(t -> t.getMachineId().equals(m.getId())).collect(
                Collectors.toList()
        );
        List<TaskUsage> queue = new LinkedList<>();
        int processed = 1;
        List<TaskUsage> predictedTaskUsageList = new ArrayList<>();
        int lastIndex = 1;
        TaskUsage lastTaskUsage = taskUsageList.get(0);
        queue.add(lastTaskUsage);
        while(processed < taskUsageList.size()){
            TaskUsage taskUsage = taskUsageList.get(lastIndex);
            if(taskUsage.getJobId() == lastTaskUsage.getJobId() &&
                    taskUsage.getTaskIndex() == lastTaskUsage.getTaskIndex()){
                queue.add(taskUsage);

            }else{
                if(!queue.isEmpty()){
                    TaskUsage predicted = (TaskUsage) PredictionFactory.getPredictionMethod("taskUsage").predict(queue);
                    predictedTaskUsageList.add(predicted);
                    queue.clear();
                }
                queue.add(taskUsage);
            }
            processed++;
            lastIndex++;
        }
        TaskUsage machineUsage = new TaskUsage();
        for(TaskUsage tu : predictedTaskUsageList){
            if(machineUsage.getEndTime() < tu.getEndTime())
                machineUsage.setEndTime(tu.getEndTime());
            if(machineUsage.getStartTime() < tu.getStartTime())
                machineUsage.setStartTime(tu.getStartTime());
            machineUsage.addTaskUsage(tu);
        }


        m.setUsagePrediction(machineUsage);
        machineRepository.save(m);

        return machineUsage;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/predict/job/{logicJobName}/{historyEnd}")
    public Duration predictJobRuntime(@PathVariable final String logicJobName, Long historyEnd) throws IOException {


        //This should be reasoned as a similarity strategy
        List<Job> jobs = jobRepository.findByLogicJobNameAndSubmitTimeLessThan(logicJobName, historyEnd);
        List<Duration> durationList = new ArrayList<Duration>();
        for(Job j : jobs){
            if(j.getFinishTime() == 0 || j.getScheduleTime() == 0 || !"finish".equals(j.getStatus()))
                continue;
            Duration duration = new Duration(j.getFinishTime() - j.getScheduleTime());
            if(duration.longValue() > 0)
                durationList.add(duration);
        }

        Duration duration = (Duration) PredictionFactory.getPredictionMethod("job").predict(durationList);
//        if(duration != null) {
//            JobRuntimePredictionController.updateJobDuration(logicJobName, duration);
//        }
        return duration;
    }

}
