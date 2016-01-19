package ro.ieat.soso.predictor;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
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
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by adrian on 11.12.2015.
 */

@RestController
public  class Predictor {

    private static final Double THRESHOLD = 0.4;
    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    MachineRepository machineRepository;
    private static Logger LOG = Logger.getLogger("Predictor");

    @RequestMapping(method = RequestMethod.PUT, path = "/predict/allUsage/{historyStart}/{historyEnd}", consumes = "application/json")
    public void predictAllMachines(@PathVariable long historyStart,@PathVariable long historyEnd){
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndEndTimeLessThan(historyStart * Configuration.TIME_DIVISOR - 1,
                        historyEnd * Configuration.TIME_DIVISOR);

        int i = 0;
        for(Machine m : machineRepository.findAll()){
            if(m.getUsagePrediction().getStartTime() == historyStart
                    && m.getUsagePrediction().getEndTime() == historyEnd){
                return;
            }


            List<TaskUsage> usageList = taskUsageList.stream().filter(t -> t.getAssignedMachineId().longValue() == m.getId() ||
                    (t.getAssignedMachineId() == 0 && t.getMachineId().equals(m.getId())))
                    .collect(Collectors.toList());



            List<TaskUsage> queue = new LinkedList<>();
            int processed = 1;
            List<TaskUsage> predictedTaskUsageList = new ArrayList<>();
            int lastIndex = 1;
            if(usageList.size() > 0){
                TaskUsage lastTaskUsage = usageList.get(0);

                queue.add(lastTaskUsage);
                while(processed < usageList.size()){
                    TaskUsage taskUsage = usageList.get(lastIndex);
                    if(taskUsage.getJobId() == lastTaskUsage.getJobId() &&
                            taskUsage.getTaskIndex() == lastTaskUsage.getTaskIndex()){
                        queue.add(taskUsage);

                    }else{
                        if(!queue.isEmpty()){
                            TaskUsage predicted = (TaskUsage) PredictionFactory.getPredictionMethod("machine").predict(queue);
                            predictedTaskUsageList.add(predicted);
                            queue.clear();
                        }
                        queue.add(taskUsage);
                    }
                    processed++;
                    lastIndex++;
                }
            }else{
                predictedTaskUsageList.add(new TaskUsage());
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
            Double availableCPU = m.getCpu() - m.getUsagePrediction().getMaxCpu();
            Double availableMem = m.getMemory() - m.getUsagePrediction().getMaxMemory();

            if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){
                m.setETA(machineUsage.getStartTime());
            }else{
                m.setETA(Long.MAX_VALUE);
            }
            machineRepository.save(m);
        }
        Logger.getLogger("Predictor").info("machines with usage: " + i);


    }



    @RequestMapping(method = RequestMethod.POST, path = "/predict/usage/{historyStart}/{historyEnd}", consumes = "application/json")
    public TaskUsage predictMachineUsage(@RequestBody Machine m, @PathVariable long historyStart,
                                                 @PathVariable long historyEnd) throws IOException, InterruptedException {


        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndEndTimeLessThan(historyStart, historyEnd);

        //only take into account tasks that were scheduled on this machine or were monitored here and haven't
        //been rescheduled.

        List<TaskUsage> usageList = taskUsageList.stream().filter(t -> t.getAssignedMachineId() == m.getId() ||
                (t.getAssignedMachineId() == 0 && t.getMachineId().equals(m.getId())))
                .collect(Collectors.toList());
        List<TaskUsage> queue = new LinkedList<>();
        int processed = 1;
        List<TaskUsage> predictedTaskUsageList = new ArrayList<>();
        int lastIndex = 1;
        TaskUsage lastTaskUsage = usageList.get(0);
        queue.add(lastTaskUsage);
        while(processed < usageList.size()){
            TaskUsage taskUsage = usageList.get(lastIndex);
            if(taskUsage.getJobId() == lastTaskUsage.getJobId() &&
                    taskUsage.getTaskIndex() == lastTaskUsage.getTaskIndex()){
                queue.add(taskUsage);

            }else{
                if(!queue.isEmpty()){
                    TaskUsage predicted = (TaskUsage) PredictionFactory.getPredictionMethod("machine").predict(queue);
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
        Double availableCPU = m.getCpu() - m.getUsagePrediction().getMaxCpu();
        Double availableMem = m.getMemory() - m.getUsagePrediction().getMaxMemory();

        if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){
            m.setETA(machineUsage.getStartTime());
        }
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

        return (Duration) PredictionFactory.getPredictionMethod("job").predict(durationList);
    }

}
