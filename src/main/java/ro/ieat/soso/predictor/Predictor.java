package ro.ieat.soso.predictor;


import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.*;
import ro.ieat.soso.core.prediction.Prediction;
import ro.ieat.soso.predictor.prediction.PredictionFactory;
import ro.ieat.soso.reasoning.CoalitionResolutor;
import ro.ieat.soso.reasoning.controllers.MachineUsagePredictionController;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Created by adrian on 11.12.2015.
 */
public class Predictor {

    public static int runWithCoalitions(String[] args) throws Exception {
        String machineUsagePath = Configuration.MACHINE_USAGE_PATH;
        String jobHistoryPath = Configuration.JOB_EVENTS_PATH;
        String machineEventsPath = Configuration.MACHINE_EVENTS;
        String taskEventsPath = Configuration.TASK_EVENTS_PATH;
        long startTime = Long.parseLong(args[0]);
        long endTime = Long.parseLong(args[1]);
        String coalitionFolder = args[2];
        String scheduledCoalition = args[3];
        String output = args[4];

        Logger LOG = Logger.getLogger(Predictor.class.toString());

        LOG.info("Starting machine mapping");
        MachineEventsMapper.map(new FileReader(machineEventsPath), startTime, endTime);
        LOG.info("Done.");

        File dir = new File(jobHistoryPath);
        Map<Long, Job> jobMap = new TreeMap<Long, Job>();
        LOG.info("Starting job events mapping");
        if(dir.isDirectory()){
            for(File f : dir.listFiles()) {
                JobEventsMapper.map(new FileReader(f), jobMap, startTime, endTime);
            }
        }else {
            JobEventsMapper.map(new FileReader(jobHistoryPath), jobMap, startTime, endTime);
        }
        LOG.info("Done.");

        LOG.info("Starting task events mapping");
        dir = new File(taskEventsPath);
        if(dir.isDirectory()){
            for(File f : dir.listFiles()) {
                TaskEventsMapper.map(new FileReader(f), jobMap, startTime, endTime);
            }
        }else{
            TaskEventsMapper.map(new FileReader(taskEventsPath), jobMap, startTime, endTime);
        }
        LOG.info("Done.");

        LOG.info("Starting job combining");
        for (Long key : jobMap.keySet()){

            JobCombiner.reduce(key, jobMap.get(key));
        }
        LOG.info("Done.");

        Map<Long, Coalition> coalitionMap;

        LOG.info("Starting coalition mapping");
        coalitionMap = CoalitionMapper.map(coalitionFolder);
        LOG.info("Done.");

        Map<Long, Coalition> scheduledCoalitions = CoalitionMapper.map(scheduledCoalition);

        for(Long id : scheduledCoalitions.keySet()){
            coalitionMap.put(id, scheduledCoalitions.get(id));
        }


        CoalitionResolutor.OUTPUT_PATH = output + "coalitions/";
        CoalitionResolutor.JOBS = jobMap;
        LOG.info("coalitions size: " + coalitionMap.size());
        LOG.info("Starting to update and write coalitions...");
        int p = 1;
        for(Coalition coalition : coalitionMap.values()) {
            CoalitionResolutor.resolute(coalition, startTime, endTime);
            if (p % coalitionMap.size() / 4 == 0)
                System.out.printf("%.4f %%\n", (p++ / (double) coalitionMap.size()) * 100);
        }
        LOG.info("Done...");




        return 0;

    }

    public static int predictMachineUsage(long machineId, long historyStart, long historyEnd) throws IOException, InterruptedException {

        String path = Configuration.MACHINE_USAGE_PATH + "/" + machineId;
        Machine m = MachineUsageMapper.readOne(new File(path), historyStart, historyEnd);
        List<TaskUsage> machineUsage =  m.getTaskUsageList();

        List<Usage> usageList = new ArrayList<Usage>();
        for(TaskUsage taskUsage : m.getTaskUsageList()){
            Long jobId = taskUsage.getJobId();
            //System.out.println(jobId + "\t" + machineProperties.getMachineId());

            for(Usage usage : taskUsage.getUsageList()){

                System.out.printf("%d %d\n", usage.getStartTime(), usage.getEndTime());
                System.out.printf("%.4f %.4f\n", usage.getCpu(), usage.getMaxCpu());
                System.out.printf("%.4f %.4f\n", usage.getMemory(), usage.getMaxMemory());

                usageList.add(usage);

            }
        }

        System.out.println("--------------------------");
        for(Usage u : usageList){
            System.out.printf("%d %d\n", u.getStartTime(), u.getEndTime());
            System.out.printf("%.4f %.4f\n", u.getCpu(), u.getMaxCpu());
            System.out.printf("%.4f %.4f\n", u.getMemory(), u.getMaxMemory());

        }

        Prediction<Double> cpuPrediction =  PredictionFactory.predictCPU(usageList);
        Prediction<Double> memPrediction =  PredictionFactory.predictMemory(usageList);
        cpuPrediction.setStartTime(historyEnd * Configuration.TIME_DIVISOR);
        cpuPrediction.setEndTime((historyEnd + Configuration.STEP) * Configuration.TIME_DIVISOR);
        memPrediction.setStartTime(historyEnd * Configuration.TIME_DIVISOR);
        memPrediction.setEndTime((historyEnd + Configuration.STEP) * Configuration.TIME_DIVISOR);
        m.setEstimatedCPULoad(cpuPrediction);
        m.setEstimatedMemoryLoad(memPrediction);
        System.out.printf("%d %d\n", cpuPrediction.getStartTime(), cpuPrediction.getEndTime());
        System.out.printf("max\tmin\tavg\thist\n");
        System.out.printf("%.4f, %.4f, %.4f, %.4f\n", cpuPrediction.getMax(), cpuPrediction.getMin(), cpuPrediction.getAverage(), cpuPrediction.getHistogram());
        System.out.println(m.getCpu());


        //Eventually this would become a call via REST
        MachineUsagePredictionController.updateMachineStatus(m.getId(), m);


        return 0;
    }

    public static int predictJobRuntime(String logicJobName, long historyStart, long historyEnd) throws IOException {

        String jobsPath = "./data/s_jobs.csv";

        List<Job> jobs = JobReader.getJobsWithLogicJobName(jobsPath, logicJobName, historyStart, historyEnd);
        List<Long> durationList = new ArrayList<Long>();
        for(Job j : jobs){
            Long duration = j.getFinishTime() - j.getScheduleTime();
            if(duration > 0)
                durationList.add(duration);
        }

        Prediction<Long> duration = PredictionFactory.predictTime(durationList);
        System.out.printf("max min avg hist\n");
        System.out.printf("%d %d %.4f %d\n", duration.getMax(), duration.getMin(), duration.getAverage(), duration.getHistogram());
        return 0;
    }

    public static int run(String[] args) throws Exception {
        String machineUsagePath = Configuration.MACHINE_USAGE_PATH;
        String jobHistoryPath = Configuration.JOB_EVENTS_PATH;
        String machineEventsPath = Configuration.MACHINE_EVENTS;
        String taskEventsPath = Configuration.TASK_EVENTS_PATH;
        long startTime = Long.parseLong(args[0]);
        long endTime = Long.parseLong(args[1]);
        String output = args[2];


        Logger LOG = Logger.getLogger(Predictor.class.toString());

        LOG.info("Starting machine mapping");
        MachineEventsMapper.map(new FileReader(machineEventsPath), startTime, endTime);
        LOG.info("Done.");

        File dir = new File(jobHistoryPath);
        Map<Long, Job> jobMap = new TreeMap<Long, Job>();
        LOG.info("Starting job events mapping");
        if(dir.isDirectory()){
            for(File f : dir.listFiles()) {
                JobEventsMapper.map(new FileReader(f), jobMap, startTime, endTime);
            }
        }else {
            JobEventsMapper.map(new FileReader(jobHistoryPath), jobMap, startTime, endTime);
        }
        LOG.info("Done.");

        LOG.info("Starting task events mapping");
        dir = new File(taskEventsPath);
        if(dir.isDirectory()){
            for(File f : dir.listFiles()) {
                TaskEventsMapper.map(new FileReader(f), jobMap, startTime, endTime);
            }
        }else{
            TaskEventsMapper.map(new FileReader(taskEventsPath), jobMap, startTime, endTime);
        }
        LOG.info("Done.");

        LOG.info("Starting job combining");
        for (Long key : jobMap.keySet()){

            JobCombiner.reduce(key, jobMap.get(key));
        }
        LOG.info("Done.");

        List<Machine> machineProperties = new ArrayList<Machine>();
        LOG.info("Starting machine usage mapping");
        dir = new File(machineUsagePath);
        if(dir.isDirectory()){
            for(File f : dir.listFiles()) {
                MachineUsageMapper.map(f, machineProperties, startTime, endTime);
            }
        }else{
            MachineUsageMapper.map(new File(machineEventsPath), machineProperties,startTime, endTime);
        }
        LOG.info("Done.");

        //TODO take this as arg
        CoalitionResolutor.OUTPUT_PATH = output + "coalitions/";
        CoalitionResolutor.JOBS = jobMap;
        LOG.severe("machines size: " + machineProperties.size());
        Map<Integer, List<Coalition>> coalitionMap = new TreeMap<Integer, List<Coalition>>();
        LOG.info("Starting to resolve coalitions...");
        int p = 1;
        for(Machine mp : machineProperties){
            CoalitionResolutor.resolute(mp, coalitionMap);
//            if(p % 20 == 0)
//                System.out.println( (p / (double) machineProperties.size()) + " %");
            p++;
        }
        LOG.info("Beginning to write coalitions...");
        int k = 1;
        int size = coalitionMap.keySet().size();
        for(int i : coalitionMap.keySet()){
            CoalitionResolutor.writeCoalitions(coalitionMap.get(i));
            System.out.printf("\r %.4f %%\n",  (k++ / (double) size)*100);
        }
        LOG.info("Done.");

        return 0;
    }
}
