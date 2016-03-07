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
import ro.ieat.soso.util.TaskUsageCombiner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
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
    private Map<String, Map<Long, ScheduledJob>> scheduledJobMap = new TreeMap<>();
    Map<Long, Job> preScheduledJobs = null;
    Logger LOG = Logger.getLogger("FineTuner");

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



    public boolean isTaskScheduledOnMachine(Long jobId, Long taskIndex, Long taskMachineId, Long machineId,
                                            List<ScheduledJob> list, String type) {
        if(scheduledJobMap.get(type) == null) {
            scheduledJobMap.put(type, new TreeMap<>());
        }

            if (!scheduledJobMap.get(type).containsKey(list.get(0).getJobId())) {
                list.forEach(s -> scheduledJobMap.get(type).put(s.getJobId(), s));
            }
            return isTaskScheduled(jobId, taskIndex, taskMachineId, machineId, scheduledJobMap.get(type));


    }

    private boolean isTaskScheduled(Long jobId, Long taskIndex, Long taskMachineId, Long machineId, Map<Long, ScheduledJob> scheduledJobMap) {
        ScheduledJob scheduledJob = scheduledJobMap.get(jobId);
        if(scheduledJob != null) {
            return scheduledJob.getTaskMachineMapping().get(taskIndex).equals(machineId);
        }
        else{
            Job j = preScheduledJobs.get(jobId);
            if(j != null){
                return  taskMachineId.equals(machineId);
            }
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
        List<TaskUsage> allTaskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndEndTimeLessThan(lowTime - 1, time + 1);
        LOG.info("This window tasks number: " + allTaskUsageList.size());

        Map<String, Map<Long, TaskUsage>> loadMap = new TreeMap<>();
        Map<Long, TaskUsage> loadMapRandom = new TreeMap<>();

//        Map<Long, UsageError> usageErrorMap = new TreeMap<>();
//        Map<Long, UsageError> usageErrorMapRandom = new TreeMap<>();

        Long[] schedulingErrors = {0L, 0L, 0L};
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

        String[] types = {"rb-tree", "linear", "random"};

        long totalTasks = 0;

        for(Job j : jobList){
            if(j.getStatus() != null)
                totalTasks += j.getTaskHistory().size();
        }

        final Long finalTime = time;
        Map<String, List<ScheduledJob>> scheduledJobs = new TreeMap<>();
        for(String type : types){
            scheduledJobs.put(type,
                    scheduledRepository.findByScheduleType(type).stream().filter(s ->
                            s.getTimeToStart() > (finalTime - Configuration.STEP *Configuration.TIME_DIVISOR))
                            .collect(Collectors.toList()));
            LOG.severe(String.format("%s Scheduled Jobs: %d \n", type, scheduledJobs.get(type).size()));
        }



        if(preScheduledJobs == null){
            preScheduledJobs = new TreeMap<>();
            jobRepository.findBySubmitTimeBetween(-1L, App.jobSendingTime * Configuration.TIME_DIVISOR).forEach(
                    j -> preScheduledJobs.put(j.getJobId(), j)
            );
            LOG.info("Prescheduled jobs = " + preScheduledJobs.size());
        }

//        scheduledRepository.delete(scheduledJobs);
//        scheduledRepository.delete(scheduledJobsRandom);
        Map<String, List<Long>> latenessMap = new TreeMap<>();
        Map<String, Long> scheduledTasks = new TreeMap<>();
        for(String type : types){
            latenessMap.put(type, new ArrayList<>());
            scheduledTasks.put(type, 0L);
            loadMap.put(type, new TreeMap<>());
        }


        for(String type : types) {
            for (ScheduledJob j : scheduledJobs.get(type)) {
                long real = getJobScheduleTime(jobList, j.getJobId());
                if (real == 0)
                    continue;

                latenessMap.get(type).add(j.getTimeToStart() - real);
                Long l = scheduledTasks.get(type) + j.getTaskMachineMapping().size();
                scheduledTasks.put(type, l);

            }
        }

        for(Machine m : machineRepository.findAll()){


//            LOG.info("Computing usage");
//            long filterTime = System.currentTimeMillis();
            Map<String, List<TaskUsage>> usageMap = new TreeMap<>();
            Map<String, TaskUsage> machineLoad = new TreeMap<>();
            Map<String, TaskUsage> machineUsage = new TreeMap<>();
            int typeNo = 0;
            for(String type : types) {
                usageMap.put(type, allTaskUsageList.stream().filter(t ->
                        isTaskScheduledOnMachine(t.getJobId(), t.getTaskIndex(), t.getMachineId(), m.getId(), scheduledJobs.get(type), type))
                        .collect(Collectors.toList()));
                 machineLoad.put(type, TaskUsageCombiner.
                        combineTaskUsageList(usageMap.get(type), lowTime));
                machineUsage.put(type, new TaskUsage());
                machineUsage.get(type).addTaskUsage(machineLoad.get(type));
//            LOG.info("Done in " + (System.currentTimeMillis() - filterTime) + " s.");
//            LOG.info("Usage size: rb-tree/random" + usageList.size() + " / " + usageListRandom.size());
//            List<TaskUsage> usageWithoutScheduled = usageList.stream().filter(t -> !jobListContainsId(jobList, t.getId()))
//                    .collect(Collectors.toList());
                machineLoad.get(type).divideCPU(m.getCpu());
                machineLoad.get(type).divideMemory(m.getMemory());
                loadMap.get(type).put(m.getId(), machineLoad.get(type));

                //overcommit
                int i = usageMap.get(type).size() - 1;
                //actually makes sense to subtract usage of task which produced error.

                boolean overcommit = false;
                while((machineUsage.get(type).getCpu() > THRESHOLD * m.getCpu() ||
                        machineUsage.get(type).getMemory() > THRESHOLD * m.getMemory()) && i >= 0) {
                    LOG.info(type + ":\nMachine usage" + machineUsage.get(type).getCpu() + " " + machineUsage.get(type).getMemory() +
                            "\nMachine capacity " + m.getCpu() + " " + m.getMemory());

                    machineUsage.get(type).substractTaskUsage(usageMap.get(type).get(i));
                    schedulingErrors[typeNo]++;

                    i--;
                    overcommit = true;
                }

                if(overcommit) {
                    logOvercommit(LOG, i);
                }
                typeNo++;
            }

        }


        long computeMeasurementTime = System.currentTimeMillis();
        LOG.info("Computing time: " + (computeMeasurementTime - measurementTime) / 1000);
//        LOG.info("Writing usage error.");
//        writeUsageError(usageErrorMap, time);

        int i = 0;
        for(String type : types) {
            LOG.info("Writing load.");
            writeLoad(loadMap.get(type), time, type);

            LOG.info("Writing scheduling errors.");
            writeScheduleErrors(schedulingErrors[i], scheduledTasks.get(type), totalTasks, time, type);


            Long[] idleCoalitions = {0L, 0L, 0L};
            List<Coalition> coalitions = coalitionRepository.findAll();
            for (Coalition c : coalitions) {
                Long[] totalIdle = {0L, 0L, 0L};

                for (Machine machineId : c.getMachines()) {
                    if (loadMap.get(type).get(machineId.getId()).getCpu() < IDLE_THRESHOLD)
                        totalIdle[i]++;
                }
                if (totalIdle[i] == c.getMachines().size())
                    idleCoalitions[i]++;
            }


            LOG.info("Writing idle coalitions");
            writeIdleCoalition(idleCoalitions[i], coalitions.size(), time, type);
            writeLateness(latenessMap, time);
            i++;

        }
        LOG.info("Finished writing: " + (System.currentTimeMillis() - computeMeasurementTime) / 1000);


    }

    private void logOvercommit(Logger LOG, int i) {
        if (i != 0) {
            LOG.info("Substracted machine usage");
        } else {
            LOG.info("Machine was overcommited!");
        }
    }

    private void writeLateness(Map<String, List<Long>> latenessMap, Long time) {
        Long[] lateTotal = {0L, 0L, 0L};
        Double[] averageRError = {.0, .0, .0};
        int i  = 0;
        Set<String> types = latenessMap.keySet();
        for(String type : types) {
            for(Long l : latenessMap.get(type)) {
                lateTotal[i] += l;
            }
            averageRError[i] = lateTotal[i] * 1.0/ latenessMap.get(type).size();
            i++;
        }

        File f = new File(testOutputPath + "lateness");
        boolean writeHeader = !f.exists();
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(f, true);
            if(writeHeader) {
                fileWriter.write("%time ");
                for(String type : types) {
                    fileWriter.write(type + "_avg " + type + "_total ");
                }
                fileWriter.write("\n");
            }
            fileWriter.write(time + " ");
            for(i = 0; i < averageRError.length; i++) {
                fileWriter.write(String.format("%.4f %d ", averageRError[i], lateTotal[i]));
            }
            fileWriter.write("\n");
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

    private void writeLoad(Map<Long, TaskUsage> loadMap, Long time, String type){
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

    private void writeIdleCoalition(Long idleCoalitions, Integer coalitionSize, Long time, String type){
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

    private void writeScheduleErrors(Long schedulingErrors, Long scheduledTasks,Long totalTasks, Long time, String type){
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


    private void writeJobRuntimeError(List<Long> runtimeErrors, Long time) {


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
