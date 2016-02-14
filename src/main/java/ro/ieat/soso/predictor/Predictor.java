package ro.ieat.soso.predictor;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.Duration;
import ro.ieat.soso.persistence.JobDurationRepository;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.predictor.prediction.JobDuration;
import ro.ieat.soso.predictor.prediction.PredictionFactory;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;

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
    @Autowired
    JobDurationRepository jobDurationRepository;
    private static Logger LOG = Logger.getLogger("Predictor");

    @RequestMapping(method = RequestMethod.PUT, path = "/predict/allUsage/{historyStart}/{historyEnd}", consumes = "application/json")
    public void predictAllMachines(@PathVariable long historyStart,@PathVariable long historyEnd){
        //Here again just endTime should be taken into account
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndEndTimeLessThan(historyStart * Configuration.TIME_DIVISOR - 1,
                        historyEnd * Configuration.TIME_DIVISOR + 1);

        long start = System.currentTimeMillis();

        int i = 0;
        List<Machine> machines = machineRepository.findAll();
        for(Machine m : machines){
            if(m.getUsagePrediction() != null) {
                if (m.getUsagePrediction().getStartTime() == historyStart
                        && m.getUsagePrediction().getEndTime() == historyEnd) {
                    return;
                }
            }


            List<TaskUsage> usageList = taskUsageList.stream().filter(t -> t.getAssignedMachineId().longValue() == m.getId() ||
                    (t.getAssignedMachineId() == 0 && t.getMachineId().equals(m.getId())))
                    .collect(Collectors.toList());



            List<TaskUsage> predictedTaskUsageList = new ArrayList<>();
            int lastIndex = 0;

            if(usageList.size() > 0){

                List<TaskPair> processedTasks = new ArrayList<>();

                while(lastIndex < usageList.size()){
                    TaskUsage taskUsage = usageList.get(lastIndex);
                    TaskPair tp = new TaskPair();
                    tp.jobId = taskUsage.getJobId();
                    tp.taskIndex = taskUsage.getTaskIndex();

                    if(processedTasks.contains(tp)) {
                        lastIndex++;
                        continue;
                    }

                    List<TaskUsage> thisTaskUsageList = usageList.stream().filter(t -> t.getJobId() == taskUsage.getJobId() &&
                        t.getTaskIndex() == taskUsage.getTaskIndex()).collect(Collectors.toList());

                    TaskUsage predicted = (TaskUsage) PredictionFactory.getPredictionMethod("machine").predict(thisTaskUsageList);
                    predictedTaskUsageList.add(predicted);

                    lastIndex++;
                    processedTasks.add(tp);
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
            Double availableCPU = m.getCpu() - m.getUsagePrediction().getCpu();
            Double availableMem = m.getMemory() - m.getUsagePrediction().getMemory();

            if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){
                m.setETA(machineUsage.getStartTime());
            }else{
                m.setETA(Long.MAX_VALUE);
            }
//            LOG.info(usageList.size() + " Task usage prediction per machine (ms): " + (System.currentTimeMillis() - start) );
        }
        machineRepository.save(machines);
        LOG.info("machines with usage: " + i);
        LOG.info("Prediction time: " + (System.currentTimeMillis() - start));


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
        Double availableCPU = m.getCpu() - m.getUsagePrediction().getCpu();
        Double availableMem = m.getMemory() - m.getUsagePrediction().getMemory();

        if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){
            m.setETA(machineUsage.getStartTime());
        }
        machineRepository.save(m);

        return machineUsage;
    }

    @RequestMapping(method = RequestMethod.PUT, path = "/predict/job/l/{logicJobName}/{historyEnd}")
    public void predictJobRuntime(@PathVariable final String logicJobName, Long historyEnd) throws IOException {


        //This should be reasoned as a similarity strategy
        List<Job> jobs = jobRepository.findByLogicJobNameAndSubmitTimeLessThan(logicJobName, historyEnd);

        JobDuration duration = computeJobDuration(jobs);
        duration.setLogicJobName(logicJobName);
        jobDurationRepository.save(duration);
    }

    public JobDuration computeJobDuration(List<Job> jobs){
        List<Duration> durationList = new ArrayList<Duration>();
        for(Job j : jobs){
            if(j.getFinishTime() == 0 || j.getScheduleTime() == 0 || !" ".equals(j.getStatus()))
                continue;
            Duration duration = new Duration(j.getFinishTime() - j.getScheduleTime());
            if(duration.longValue() > 0)
                durationList.add(duration);
        }
        if(durationList.size() > 0) {
            Duration d = (Duration) PredictionFactory.getPredictionMethod("job").predict(durationList);
            JobDuration duration = new JobDuration();
            duration.setDuration(d);
            return duration;
        }
        return null;
    }

    @RequestMapping(method = RequestMethod.PUT, path = "/predict/job/{historyStart}/{historyEnd}")
    public void predictAllJobsRuntime(@PathVariable final Long historyStart,@PathVariable final Long historyEnd) throws IOException {
        List<Job> all = jobRepository.findBySubmitTimeBetween(historyStart, historyEnd * Configuration.TIME_DIVISOR);
        List<String> logicJobNames = new ArrayList<>();
        all.stream().forEach((job) -> {
            if(!logicJobNames.contains(job.getLogicJobName()))
                logicJobNames.add(job.getLogicJobName());
        });
        List<JobDuration> durations = new ArrayList<>();
        LOG.info("Logic job name list size: " + logicJobNames.size());
        LOG.info("All jobs name list size: " + all.size());

        for(String logicJobName : logicJobNames){
            List<Job> jobs = all.stream().filter(j ->
                    j.getSubmitTime() < historyEnd && j.getLogicJobName().equals(logicJobName))
                    .collect(Collectors.toList());
            JobDuration duration = computeJobDuration(jobs);
            if(duration != null) {
                duration.setLogicJobName(logicJobName);
                durations.add(duration);
            }
        }

        jobDurationRepository.save(durations);
        CoalitionClient client = new CoalitionClient();
        client.sendJobRuntimePrediction(durations);
    }


    private static class TaskPair{
        public long jobId;
        public long taskIndex;

        public boolean equals(Object o ){
            if(o instanceof TaskPair){
                TaskPair t = (TaskPair) o;
                return t.jobId == this.jobId && t.taskIndex == this.taskIndex;
            }
            return false;
        }
    }

}
