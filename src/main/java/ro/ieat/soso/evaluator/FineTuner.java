package ro.ieat.soso.evaluator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.App;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
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

    @Autowired
    ScheduledRepository scheduledRepository;
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



    public boolean isTaskScheduledOnMachine(long jobId, Long taskIndex, Long machineId, List<ScheduledJob> list) {
        for(ScheduledJob scheduledJob : list){
            if(scheduledJob.getJobId() == jobId)
                return scheduledJob.getTaskMachineMapping().get(taskIndex).equals(machineId);
        }
        return false;

    }

    public boolean isJobScheduled(Long time, Long jobId, List<ScheduledJob> list){
        if (time < App.jobSendingTime)
            return true;
        for(ScheduledJob scheduledJob : list){
            if(scheduledJob.getJobId() == jobId &&
                    scheduledJob.getTimeToStart() > time - Configuration.STEP * Configuration.TIME_DIVISOR &&
                    scheduledJob.getTimeToStart() < time)
                return true;
        }
        return false;
    }

    private static long getJobScheduleTime(List<Job> list, long jobId){
        for(Job j : list)
            if (j.getJobId() == jobId)
                return j.getScheduleTime();
        return 0;
    }


    @RequestMapping(method = RequestMethod.PUT, path = "/finetuner/{time}")
    public void fineTuneAndWriteResults(@PathVariable  Long time){

        long measurementTime = System.currentTimeMillis();

        long lowTime = (time - Configuration.STEP) * Configuration.TIME_DIVISOR;
        time *= Configuration.TIME_DIVISOR;
        Logger LOG = Logger.getLogger("FineTuner");
        List<TaskUsage> allTaskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndEndTimeLessThan(lowTime - 1, time + 1);
        LOG.info("This window tasks number: " + allTaskUsageList.size());

        Map<Long, TaskUsage> loadMap = new TreeMap<>();
        Map<Long, TaskUsage> loadMapRandom = new TreeMap<>();

//        Map<Long, UsageError> usageErrorMap = new TreeMap<>();
//        Map<Long, UsageError> usageErrorMapRandom = new TreeMap<>();

        Long schedulingErrors = 0L;
        Long schedulingErrorsRandom = 0L;
        List<Job> jobList = jobRepository.findBySubmitTimeBetween(lowTime-1, time+1);
//        List<JobDuration> jobDurations = jobDurationRepository.findBySubmitTimeBetween(lowTime, time);
//        List<Long> runtimeErrors = new ArrayList<>();
//        for(Job j : jobList){
//            Long d = j.getFinishTime() - j.getScheduleTime();
//            scheduledTasks += j.getTaskHistory().size();
//            for(JobDuration jd : jobDurations){
//                if(jd.getLogicJobName().equals(j.getLogicJobName())){
//                    runtimeErrors.add(Math.abs(jd.getDuration().longValue() - d));
//                }
//            }
//        }

        long scheduledTasks = 0;
        long scheduledTasksRandom = 0;
        long totalTasks = 0;

        for(Job j : jobList){
            if(j.getStatus() != null)
                totalTasks += j.getTaskHistory().size();
        }

        final Long finalTime = time;
        List<ScheduledJob> allscheduledJobs = scheduledRepository.findByScheduleType("rb-tree");
        List<ScheduledJob> allscheduledJobsRandom = scheduledRepository.findByScheduleType("random");
        List<ScheduledJob> scheduledJobs = allscheduledJobs.stream().filter(s ->
                s.getTimeToStart() > (finalTime - Configuration.STEP *Configuration.TIME_DIVISOR))
                .collect(Collectors.toList());
        List<ScheduledJob> scheduledJobsRandom = allscheduledJobsRandom.stream().filter(s ->
                s.getTimeToStart() > (finalTime - Configuration.STEP *Configuration.TIME_DIVISOR))
                .collect(Collectors.toList());
        LOG.severe("Scheduled jobs:\nrandom: " + scheduledJobsRandom.size() + "\nrb-tree: " + scheduledJobs.size());

//        scheduledRepository.delete(scheduledJobs);
//        scheduledRepository.delete(scheduledJobsRandom);
        List<Long> latenessList = new ArrayList<>();
        List<Long> latenessListRandom = new ArrayList<>();
        for(ScheduledJob j : scheduledJobs){
            long real = getJobScheduleTime(jobList, j.getJobId());
            if(real == 0)
                continue;

            latenessList.add(j.getTimeToStart() - real);
            scheduledTasks += j.getTaskMachineMapping().size();

        }

        for(ScheduledJob j : scheduledJobsRandom){
            long real = getJobScheduleTime(jobList, j.getJobId());
            if(real == 0)
                continue;
            latenessListRandom.add(j.getTimeToStart() - real);
            scheduledTasksRandom += j.getTaskMachineMapping().size();
        }

        for(Machine m : machineRepository.findAll()){


            LOG.info("Computing usage");
            long filterTime = System.currentTimeMillis();
            List<TaskUsage> usageList = allTaskUsageList.stream().filter(t ->
                            isTaskScheduledOnMachine(t.getJobId(), t.getTaskIndex(), m.getId(), allscheduledJobs))
                    .collect(Collectors.toList());


            List<TaskUsage> usageListRandom = allTaskUsageList.stream().filter(t ->
                    (isTaskScheduledOnMachine(t.getJobId(), t.getTaskIndex(), m.getId(), allscheduledJobsRandom)))
                    .collect(Collectors.toList());

            LOG.info("Done in " + (System.currentTimeMillis() - filterTime) + " s.");
            LOG.info("Usage size: rb-tree/random" + usageList.size() + " / " + usageListRandom.size());
//            List<TaskUsage> usageWithoutScheduled = usageList.stream().filter(t -> !jobListContainsId(jobList, t.getId()))
//                    .collect(Collectors.toList());




            TaskUsage machineLoad = TaskUsageCombiner.
                    combineTaskUsageList(usageList, lowTime, jobList, scheduledJobs, "rb-tree");
//            TaskUsage machineLoadWithoutCurrent = TaskUsageCombiner.
//                    combineTaskUsageList(usageWithoutScheduled, lowTime, jobList, scheduledJobs, "rb-tree");

            TaskUsage machineLoadRandom = TaskUsageCombiner.
                    combineTaskUsageList(usageListRandom, lowTime, jobList, scheduledJobsRandom, "random");
//            TaskUsage machineLoadWithoutCurrentRandom = TaskUsageCombiner.
//                    combineTaskUsageList(usageWithoutScheduled, lowTime, jobList, scheduledJobs, "random");

            TaskUsage machineUsage = new TaskUsage();
            machineUsage.addTaskUsage(machineLoad);

            TaskUsage machineUsageRandom = new TaskUsage();
            machineUsageRandom.addTaskUsage(machineLoadRandom);

            machineLoad.divideCPU(m.getCpu());
            machineLoadRandom.divideCPU(m.getCpu());

            machineLoad.divideMemory(m.getMemory());
            machineLoadRandom.divideMemory(m.getMemory());

            loadMap.put(m.getId(), machineLoad);
            loadMapRandom.put(m.getId(), machineLoadRandom);


//            usageErrorMap.put(m.getId(), new UsageError(machineLoadWithoutCurrent, m.getUsagePrediction()));
//            usageErrorMapRandom.put(m.getId(), new UsageError(machineLoadWithoutCurrentRandom, m.getUsagePrediction()));

            int i = usageList.size() - 1;
            //actually makes sense to subtract usage of task which produced error.

            boolean overcommit = false;
            while((machineUsage.getCpu() > THRESHOLD * m.getCpu() ||
                    machineUsage.getMemory() > THRESHOLD * m.getMemory()) && i >= 0) {
                LOG.info("Machine usage" + machineUsage.getCpu() + " " + machineUsage.getMemory() +
                        "\nMachine capacity " + m.getCpu() + " " + m.getMemory());

                machineUsage.substractTaskUsage(usageList.get(i));
                schedulingErrors++;

                i--;
                overcommit = true;
            }

            if(overcommit) {
                logOvercommit(LOG, i);
            }

            overcommit = false;
            i = usageListRandom.size() - 1;
            while((machineUsageRandom.getCpu() > THRESHOLD * m.getCpu() ||
                    machineUsageRandom.getMemory() > THRESHOLD * m.getMemory()) && i >= 0) {
                LOG.info("Machine usage random" + machineUsageRandom.getCpu() + " " + machineUsageRandom.getMemory() +
                        "\nMachine capacity " + m.getCpu() + " " + m.getMemory());

                machineUsage.substractTaskUsage(usageListRandom.get(i));
                schedulingErrorsRandom++;

                i--;
                overcommit = true;
            }
            if(overcommit) {
                logOvercommit(LOG, i);
            }
        }


        long computeMeasurementTime = System.currentTimeMillis();
        LOG.info("Computing time: " + (computeMeasurementTime - measurementTime) / 1000);
//        LOG.info("Writing usage error.");
//        writeUsageError(usageErrorMap, time);

        LOG.info("Writing load.");
        writeLoad(loadMap, time, "rb-tree");
        writeLoad(loadMapRandom, time, "random");

        LOG.info("Writing scheduling errors.");
        writeScheduleErrors(schedulingErrors, scheduledTasks, totalTasks, time, "rb-tree");
        writeScheduleErrors(schedulingErrorsRandom, scheduledTasksRandom, totalTasks, time, "random");

        long idleCoalitions = 0;
        long idleCoalitionsRandom = 0;
        List<Coalition> coalitions = coalitionRepository.findAll();
        for(Coalition c : coalitions){
            long totalIdle = 0;
            long totalIdleRandom = 0;
            for(Machine machineId : c.getMachines()){
                if(loadMap.get(machineId.getId()).getCpu() < IDLE_THRESHOLD)
                    totalIdle++;
                if(loadMapRandom.get(machineId.getId()).getCpu() < IDLE_THRESHOLD)
                    totalIdleRandom++;
            }
            if(totalIdle == c.getMachines().size())
                idleCoalitions++;
            if(totalIdleRandom == c.getMachines().size())
                idleCoalitionsRandom++;
        }

        LOG.info("Writing idle coalitions");
        writeIdleCoalition(idleCoalitions, coalitions.size(), time, "rb-tree");
        writeIdleCoalition(idleCoalitionsRandom, coalitions.size(), time, "random");
        writeLateness(latenessList, latenessListRandom, time);

        LOG.info("Finished writing: " + (System.currentTimeMillis() - computeMeasurementTime) / 1000);


    }

    private void logOvercommit(Logger LOG, int i) {
        if (i != 0) {
            LOG.info("Substracted machine usage");
        } else {
            LOG.info("Machine was overcommited!");
        }
    }

    private void writeLateness(List<Long> latenessList, List<Long> latenessListRandom, Long time) {
        Long lateTotal = 0L;
        for(Long l : latenessList)
            lateTotal += l;
        double averageRError = lateTotal * 1.0/ latenessList.size();
        Long lateTotalRandom = 0L;
        for(Long l : latenessListRandom)
            lateTotalRandom += l;
        double averageRErrorRandom = lateTotalRandom * 1.0/ latenessListRandom.size();

        File f = new File(testOutputPath + "lateness");
        boolean writeHeader = !f.exists();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time lateRB lateRandom totalRB totalRandom\n");
            fileWriter.write(String.format("%d %.4f %.4f %d %d\n",
                    time, averageRError, averageRErrorRandom, lateTotal, lateTotalRandom));
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

    private void writeLoad(Map<Long, TaskUsage> loadMap, long time, String type){
        //LOAD
        File dir = new File(testOutputPath + "load/" + type);
        if(!dir.exists())
            dir.mkdirs();
        File f = new File(testOutputPath + "load/" + type + "/machine");
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
            fileWriter.write("\n");
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

        f = new File(testOutputPath + "load/" + type + "/average");
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

    private void writeIdleCoalition(long idleCoalitions, int coalitionSize, long time, String type){
        //IDLE COALITIONS
        File f = new File(testOutputPath + "load/" + type + "/idle_coals");
        FileWriter fileWriter = null;
        boolean writeHeader = !f.exists();
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #idle_coals #total\n");
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

    private void writeScheduleErrors(Long schedulingErrors, long scheduledTasks,long totalTasks, long time, String type){
        File dir = new File(testOutputPath + "schedule/" + type);
        if(!dir.exists())
            dir.mkdirs();

        File f = new File(testOutputPath + "schedule/" + type + "/errors");
        boolean writeHeader = !f.exists();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time #sched_errs #scheduled_now #total\n");
            fileWriter.write(String.format("%d %d %d %d\n", time, schedulingErrors, scheduledTasks, totalTasks));
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
