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
    private static final Double THRESHOLD = 0.8;
    private static final Double IDLE_THRESHOLD = 0.1;

    RestTemplate template = new RestTemplate();
    private static final String testOutputPath = "./output/results/";


    private static void writeResults(long time, List<Double> cpuLoad, List<Double> memLoad, List<Long> lateList, long numViolations){

        double avgCPU = 0, avgMem = 0, avgLate = 0;
        for(Double d : cpuLoad)
            avgCPU+= d;
        for(Double d : memLoad)
            avgMem+= d;
        for(Long l : lateList)
            avgLate += l;


        File f = new File(testOutputPath + "initial");
        boolean writeHeader = f.exists();
        try {
            FileWriter fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time avgCPU avgMEM avgLATE numViol");
            fileWriter.write(String.format("%d %f %f %f %d", time, avgCPU/cpuLoad.size(), avgMem/memLoad.size(),
                    avgLate/lateList.size(), numViolations));

        } catch (IOException e) {
            Logger.getLogger("Evaluator").severe("File not found " + f.getAbsolutePath());
        }
    }

    @RequestMapping(method = RequestMethod.PUT, path = "/finetuner/{time}")
    public void fineTuneAndWriteResults(@PathVariable Long time){

        long lowTime = (time - Configuration.STEP) * Configuration.TIME_DIVISOR;
        Logger LOG = Logger.getLogger("FineTuner");
        List<TaskUsage> allTaskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndFinishTimeLessThan(lowTime, time);

        Map<Long, TaskUsage> loadMap = new TreeMap<>();
        Map<Long, UsageError> usageErrorMap = new TreeMap<>();
        Long schedulingErrors = 0L;
        List<Long> jobIds = new ArrayList<>();
        for(Machine m : machineRepository.findAll()){
            TaskUsage machineLoad = new TaskUsage();
            List<TaskUsage> usageList = allTaskUsageList.stream().filter(t -> t.getAssignedMachineId().longValue() == m.getId() ||
                    (t.getAssignedMachineId() == 0 && t.getMachineId().equals(m.getId())))
                    .collect(Collectors.toList());
            for(TaskUsage t : usageList) {
                jobIds.add(t.getJobId());
                machineLoad.addTaskUsage(t);
            }
            loadMap.put(m.getId(), machineLoad);
            usageErrorMap.put(m.getId(), new UsageError(machineLoad, m.getUsagePrediction()));


            int i = usageList.size() - 1;
            //actually makes sense to substract usage of task which produced error.
            while(machineLoad.getMaxCpu() > THRESHOLD * m.getCpu() ||
                    machineLoad.getMaxMemory() > THRESHOLD * m.getMemory()){
                machineLoad.substractTaskUsage(usageList.get(i));
                schedulingErrors++;
                i--;

            }
        }


        long idleCoalitions = 0;
        for(Coalition c : coalitionRepository.findAll()){
            long totalIdle = 0;
            for(Long machineId : c.getMachines()){
                if(loadMap.get(machineId).getCpu() < IDLE_THRESHOLD)
                    totalIdle++;
            }
            if(totalIdle == c.getMachines().size())
                idleCoalitions++;
        }

        List<Job> jobList = jobRepository.findBySubmitTimeBetween(lowTime, time);
        List<JobDuration> jobDurations = jobDurationRepository.findBySubmitTimeBetween(lowTime, time);
        List<Long> runtimeErrors = new ArrayList<>();
        for(Job j : jobList){
            Long d = j.getFinishTime() - j.getScheduleTime();
            for(JobDuration jd : jobDurations){
                if(jd.getLogicJobName().equals(j.getLogicJobName())){
                    runtimeErrors.add(Math.abs(jd.getDuration().longValue() - d));
                }
            }
        }

        writeResults(usageErrorMap, loadMap, idleCoalitions, schedulingErrors, runtimeErrors, time);



    }

    private void writeResults(Map<Long, UsageError> usageErrorMap, Map<Long, TaskUsage> loadMap, long idleCoalitions, Long schedulingErrors, List<Long> runtimeErrors, long time) {


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
            fileWriter.write(String.format("%d %s", time, average.toStringforPlot()));
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


        //LOAD
        f = new File(testOutputPath + "load/" + time);
        TaskUsage averageUsage = new TaskUsage();

        try {
            fileWriter = new FileWriter(f);
            fileWriter.write("%mID cpu maxCpu mem maxMem disk maxDisk");
            for(Long key : loadMap.keySet()){
                TaskUsage load = loadMap.get(key);
                averageUsage.addTaskUsage(load);
                fileWriter.write(String.format("%d %s\n", key, load.loadForPlot()));
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
                fileWriter.write("%time cpu maxCpu mem maxMem disk maxDisk");
            fileWriter.write(String.format("%d %s", time, averageUsage.loadForPlot()));
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


        //IDLE COALITIONS
        f = new File(testOutputPath + "load/idle_coals");
        writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #idle_coals");
            fileWriter.write(String.format("%d %d", time, idleCoalitions));
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

        f = new File(testOutputPath + "schedule/errors");
        writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #sched_errs");
            fileWriter.write(String.format("%d %d", time, schedulingErrors));
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

        Long runtimeError = 0L;
        for(Long l : runtimeErrors)
            runtimeError += l;
        double averageRError = runtimeError * 1.0/ runtimeErrors.size();
        f = new File(testOutputPath + "runtime/errors");
        writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #avg runtime error");
            fileWriter.write(String.format("%d %.4f", time, averageRError));
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
