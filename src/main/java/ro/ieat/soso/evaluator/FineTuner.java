package ro.ieat.soso.evaluator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.persistence.*;
import ro.ieat.soso.predictor.prediction.JobDuration;
import ro.ieat.soso.util.TaskUsageCombiner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by adrian on 13.01.2016.
 * This class is used to compute errors in predictions and to write into files the state of the system.
 */
@RestController
public class FineTuner {
    @Autowired
    CoalitionRepository coalitionRepository;
    @Autowired
    MachineRepository machineRepository;
    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    JobDurationRepository jobDurationRepository;
    private static final Double THRESHOLD = 1.0;
    private static final Double IDLE_THRESHOLD = 0.05;

    RestTemplate template = new RestTemplate();
    private static final String testOutputPath = "./output/results/";


    private boolean jobListContainsId(List<Job> list, Long id){
        for(Job j : list)
            if (j.getJobId() == id)
                return true;
        return false;
    }

    private boolean isFinishedBefore(List<Job> list, Long id, Long when){
        for(Job j : list)
            if (j.getJobId() == id) {
                if(j.getFinishTime() < when)
                    return true;
            }
        return false;
    }

    @RequestMapping(method = RequestMethod.PUT, path = "/finetuner/{time}")
    public void fineTuneAndWriteResults(@PathVariable Long time){

        long lowTime = (time - Configuration.STEP) * Configuration.TIME_DIVISOR;
        time *= Configuration.TIME_DIVISOR;
        Logger LOG = Logger.getLogger("FineTuner");
        List<TaskUsage> allTaskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndEndTimeLessThan(lowTime - 1, time + 1);
        LOG.info("This window tasks number: " + allTaskUsageList.size());

        Map<Long, TaskUsage> loadMap = new TreeMap<>();
        Map<Long, UsageError> usageErrorMap = new TreeMap<>();
        Long schedulingErrors = 0L;
        List<Long> jobIds = new ArrayList<>();
        List<Job> jobList = jobRepository.findBySubmitTimeBetween(lowTime, time);
        List<JobDuration> jobDurations = jobDurationRepository.findBySubmitTimeBetween(lowTime, time);
        List<Long> runtimeErrors = new ArrayList<>();
        long scheduledTasks = 0;
        for(Job j : jobList){
            Long d = j.getFinishTime() - j.getScheduleTime();
            scheduledTasks += j.getTaskHistory().size();
            for(JobDuration jd : jobDurations){
                if(jd.getLogicJobName().equals(j.getLogicJobName())){
                    runtimeErrors.add(Math.abs(jd.getDuration().longValue() - d));
                }
            }
        }


        for(Machine m : machineRepository.findAll()){

            List<TaskUsage> usageList = allTaskUsageList.stream().filter(t -> t.getAssignedMachineId().longValue() == m.getId() ||
                    (t.getAssignedMachineId() == 0 && t.getMachineId().equals(m.getId())))
                    .collect(Collectors.toList());
            List<TaskUsage> usageWithoutScheduled = usageList.stream().filter(t -> !jobListContainsId(jobList, t.getId()))
                    .collect(Collectors.toList());
            TaskUsage machineLoad = TaskUsageCombiner.combineTaskUsageList(usageList, lowTime);
            TaskUsage machineLoadWithoutCurrent = TaskUsageCombiner.combineTaskUsageList(usageWithoutScheduled, lowTime);

            TaskUsage machineUsage = new TaskUsage();
            machineUsage.addTaskUsage(machineLoad);
            machineLoad.divideCPU(m.getCpu());
            machineLoad.divideMemory(m.getMemory());
            loadMap.put(m.getId(), machineLoad);
            usageErrorMap.put(m.getId(), new UsageError(machineLoadWithoutCurrent, m.getUsagePrediction()));
//            LOG.info(machineLoad.getCpu() + " <- cpu");

            int i = usageList.size() - 1;
            //actually makes sense to subtract usage of task which produced error.

            while((machineUsage.getCpu() > THRESHOLD * m.getCpu() ||
                    machineUsage.getMemory() > THRESHOLD * m.getMemory()) && i >= 0) {
                LOG.info("Machine usage" + machineUsage.getCpu() + " " + machineUsage.getMemory() +
                        "\nMachine capacity " + m.getCpu() + " " + m.getMemory());

                if(usageList.get(i).getAssignedMachineId() != 0) {
                    machineUsage.substractTaskUsage(usageList.get(i));
                    schedulingErrors++;
                }
                i--;
            }
        }

        LOG.info("Writing usage error.");
        writeUsageError(usageErrorMap, time);

        LOG.info("Writing usage error.");
        writeLoad(loadMap, time);

        LOG.info("Writing scheduling errors.");
        writeScheduleErrors(schedulingErrors, scheduledTasks, time);

        long idleCoalitions = 0;
        List<Coalition> coalitions = coalitionRepository.findAll();
        for(Coalition c : coalitions){
            long totalIdle = 0;
            for(Machine machineId : c.getMachines()){
                if(loadMap.get(machineId.getId()).getCpu() < IDLE_THRESHOLD)
                    totalIdle++;
            }
            if(totalIdle == c.getMachines().size())
                idleCoalitions++;
        }

        LOG.info("Writing idle coalitions");
        writeIdleCoalition(idleCoalitions, coalitions.size(), time);


//        writeResults(usageErrorMap, loadMap, idleCoalitions, coalitions.size(), schedulingErrors, scheduledTasks, runtimeErrors, time);



    }

    private void writeUsageError(Map<Long, UsageError> usageErrorMap, long time){
        //USAGE PREDICTION ERROR
        File f = new File(testOutputPath + "usageError/" + time);
        UsageError average = UsageError.getZeroUsageError();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(f);
            fileWriter.write("%mID cpu maxCpu mem maxMem disk maxDisk");
            for(Long key : usageErrorMap.keySet()){
                UsageError error = usageErrorMap.get(key);
                average.addError(error);
                fileWriter.write(String.format("%d %s\n", key, error.toStringforPlot()));
            }
            average.divide(usageErrorMap.size());

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        f = new File(testOutputPath + "usageError/average");
        boolean writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time cpu maxCpu mem maxMem disk maxDisk");
            fileWriter.write(String.format("%d %s\n", time, average.toStringforPlot()));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private void writeLoad(Map<Long, TaskUsage> loadMap, long time){
        //LOAD
        File f = new File(testOutputPath + "load/machine");
        TaskUsage averageUsage = new TaskUsage();
        FileWriter fileWriter = null;
        boolean writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time machine cpu maxCpu mem maxMem disk maxDisk\n");
            for(Long key : loadMap.keySet()){
                TaskUsage load = loadMap.get(key);
                averageUsage.addTaskUsage(load);
                fileWriter.write(String.format("%d %d %s\n", time, key, load.loadForPlot()));
            }
            averageUsage.divide(loadMap.size());

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }

        f = new File(testOutputPath + "load/average");
        writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time cpu maxCpu mem maxMem disk maxDisk\n");
            fileWriter.write(String.format("%d %s\n", time, averageUsage.loadForPlot()));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private void writeIdleCoalition(long idleCoalitions, int coalitionSize, long time){
        //IDLE COALITIONS
        File f = new File(testOutputPath + "load/idle_coals");
        FileWriter fileWriter = null;
        boolean writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #idle_coals\n");
            fileWriter.write(String.format("%d %d %d\n", time, idleCoalitions, coalitionSize));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private void writeScheduleErrors(Long schedulingErrors, long scheduledTasks, long time){

        File f = new File(testOutputPath + "schedule/errors");
        boolean writeHeader = !f.exists();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #sched_errs #total\n");
            fileWriter.write(String.format("%d %d %d\n", time, schedulingErrors, scheduledTasks));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }


    private void writeJobRuntimeError(List<Long> runtimeErrors, long time) {


        Long runtimeError = 0L;
        for(Long l : runtimeErrors)
            runtimeError += l;
        double averageRError = runtimeError * 1.0/ runtimeErrors.size();
        File f = new File(testOutputPath + "runtime/errors");
        boolean writeHeader = !f.exists();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #avg runtime error\n");
            fileWriter.write(String.format("%d %.4f\n", time, averageRError));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(fileWriter != null)
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }


}
