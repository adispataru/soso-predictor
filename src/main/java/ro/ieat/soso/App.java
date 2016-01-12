package ro.ieat.soso;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.mappers.JobEventsMapper;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.mappers.TaskEventsMapper;
import ro.ieat.soso.core.mappers.TaskUsageMapper;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.predictor.Predictor;
import ro.ieat.soso.predictor.persistence.MachineRepository;
import ro.ieat.soso.reasoning.CoalitionReasoner;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;
import ro.ieat.soso.reasoning.controllers.persistence.CoalitionRepository;
import ro.ieat.soso.util.MapsUtil;
import ro.ieat.soso.util.TaskUsageConqueror;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Created by adrian on 05.01.2016.
 */
@SpringBootApplication
public class App {

    private static Logger LOG = Logger.getLogger(App.class.toString());

    public static void main(String[] args) throws Exception {
        Configuration.MACHINE_USAGE_PATH = "./data/output/machine_usage";
        //Predictor.predictMachineUsage(5L, 600, 900);
//        try {
//            Predictor.predictJobRuntime("D7IK6PSGY5Jcf32bkgMfNgBzrXUQs-DhLi4+jCYwZvQ=", 600, Long.MAX_VALUE / Configuration.TIME_DIVISOR);
//        } catch (Exception e) {
//            System.err.printf("Caught error: \n" + e);
//        }
        //initializeCoalitions();

        //configure(600, 6000);

        SpringApplication.run(App.class, args);
        runIndefinetely();


    }

    public static void configure(long startTime, long endTime) throws IOException, InterruptedException {

        String machineUsagePath = Configuration.MACHINE_USAGE_PATH;
        String jobHistoryPath = Configuration.JOB_EVENTS_PATH;
        String machineEventsPath = Configuration.MACHINE_EVENTS;
        String taskEventsPath = Configuration.TASK_EVENTS_PATH;

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

        LOG.info("Starting task usage mapping");
        dir = new File(Configuration.TASK_USAGE_PATH);
        if(dir.isDirectory()){
            int i = 1;
            for(File f : dir.listFiles()){
                TaskUsageMapper.map(new FileReader(f), jobMap, startTime, endTime);
                LOG.info("Processed " + i + " of " + dir.listFiles().length + " files...");
                ++i;
            }
        }else{
            TaskUsageMapper.map(new FileReader(Configuration.TASK_USAGE_PATH), jobMap, startTime, endTime);
        }
        LOG.info("Done.");
//        MachineRepository.jobRepo = jobMap;

        //Freeze!
//        for(File f : new File(Configuration.MACHINE_USAGE_PATH).listFiles()) {
//            MachineRepository.save(MachineUsageMapper.readOne(f, startTime, endTime));
//        }

    }

    public static void initializeCoalitions() throws Exception {
        String machineUsagePath = "./data/output/machine_usage/";

        File[] machineFiles = new File(machineUsagePath).listFiles();

        for(File f : machineFiles){
            Predictor.predictMachineUsage(Long.parseLong(f.getName()), 600, 5400);
        }

        Map<Long, Job> jobMap = new TreeMap<Long, Job>();
        for(File f : new File(Configuration.JOB_EVENTS_PATH).listFiles()) {
            JobEventsMapper.map(new FileReader(f), jobMap, 600, 5400);
        }
        for(File f : new File(Configuration.TASK_EVENTS_PATH).listFiles()) {
            TaskEventsMapper.map(new FileReader(f), jobMap, 600, 5400);
        }

        CoalitionReasoner.appDurationMap = new TreeMap<String, DurationPrediction>();
        for(Job j : jobMap.values()){
//            if(CoalitionReasoner.appDurationMap.containsKey(j.getLogicJobName()))
//                continue;
            Predictor.predictJobRuntime(j.getLogicJobName(), 600, 5400);
        }

        CoalitionReasoner.initCoalitions(5400);
        Collection<Coalition> cs = CoalitionRepository.coalitionMap.values();

        String coalitionOutputFolder = "./data/coalitions/";

        for(Coalition coalition : cs){
            System.out.printf("coalition id: %d; coalition size: %d\n", coalition.getId(), coalition.getMachines().size());
            ObjectMapper objectMapper = new ObjectMapper();
            File dir = new File(coalitionOutputFolder);
            if(!dir.exists())
                dir.mkdirs();


            FileWriter f = new FileWriter(coalitionOutputFolder + coalition.getId());
            f.write(objectMapper.writeValueAsString(coalition));
            f.close();
        }

    }

    public static void runIndefinetely() throws Exception {

        long initStart = 0, initEnd = 5700;
        Long maxTime = Long.MAX_VALUE / Configuration.TIME_DIVISOR;
        Map<Long, Job> jobMap = new TreeMap<Long, Job>();

        long time = System.currentTimeMillis();

        for(File f : new File(Configuration.JOB_EVENTS_PATH).listFiles()) {
            JobEventsMapper.map(new FileReader(f), jobMap, initStart, maxTime);
        }

        LOG.info("Finished job events mapping");
        for(File f : new File(Configuration.TASK_EVENTS_PATH).listFiles()) {
            TaskEventsMapper.map(new FileReader(f), jobMap, initStart, maxTime);
        }

        LOG.info("Finished task events mapping");

        MachineRepository.getInstance().jobRepo = jobMap;


        //Make use of machine_events file to populate MachineRepository;
        LOG.info("Starting machine mapping");
        MachineEventsMapper.map(new FileReader(Configuration.MACHINE_EVENTS), 0, maxTime);
        LOG.info("Done.");

        LOG.info("Generating machines based on events file");
        for(Long id : MachineEventsMapper.MACHINES.keySet()){
            Machine m = new Machine(id, MachineEventsMapper.MACHINES.get(id).getKey(),
                    MachineEventsMapper.MACHINES.get(id).getValue());
            MachineRepository.getInstance().save(m);
        }


        LOG.info("Starting task usage mapping");
        File dir = new File(Configuration.TASK_USAGE_PATH);
        if(dir.isDirectory()){
            int i = 1;
            for(File f : dir.listFiles()){
                TaskUsageConqueror.map(new FileReader(f), MachineRepository.getInstance(), 600, initEnd);
                LOG.info("Processed " + i + " of " + dir.listFiles().length + " files...");
                ++i;
            }
        }else{
            TaskUsageMapper.map(new FileReader(Configuration.TASK_USAGE_PATH), jobMap, initStart, initEnd);
        }

        LOG.info("Done.");

        long time2 = System.currentTimeMillis();
        LOG.info("Reading duration: " + (time2 - time));

//        String machineUsagePath = "./data/output/machine_usage/";
//
//        File[] machineFiles = new File(machineUsagePath).listFiles();
//
//        LOG.info("Reading and predicting machine usage from files...");
//
//        for(File f : machineFiles){
//            Machine m = MachineUsageMapper.readOne(f, initStart, initEnd);
//            MachineRepository.save(m);
//            Predictor.predictMachineUsage(Long.parseLong(f.getName()), initStart, initEnd);
//        }
//
//        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time2));

        CoalitionReasoner.appDurationMap = new TreeMap<String, DurationPrediction>();


        time = System.currentTimeMillis();
        LOG.info("Predicting machine usage...");
        for(Machine m : MachineRepository.getInstance().findAll()){
            Predictor.predictMachineUsage(m.getId(), initStart, initEnd);
        }

        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        LOG.info("Initializing coalitions...");
        CoalitionReasoner.initCoalitions(initEnd);
        for(Coalition c : CoalitionRepository.coalitionMap.values()){
            LOG.info("Id: " + c.getId());
        }



        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));


        jobMap = MapsUtil.sortJobMaponSubmitTime(jobMap);

        Iterator<Map.Entry<Long, Job>> iterator = jobMap.entrySet().iterator();
        if(MachineRepository.getInstance().jobRepo == null)
            MachineRepository.getInstance().jobRepo = new TreeMap<Long, Job>();
        while (iterator.hasNext()){
            Job j = iterator.next().getValue();
            MachineRepository.getInstance().jobRepo.put(j.getJobId(), j);
            Predictor.predictJobRuntime(j.getLogicJobName(), initStart, initEnd-300);
            if(j.getSubmitTime() >= (initEnd) * Configuration.TIME_DIVISOR){
                CoalitionClient.sendJobRequest(new Job(j, true));

                break;
            }

        }


        long someTime = 7000;

        while (iterator.hasNext()){
            Job j = iterator.next().getValue();
            Predictor.predictJobRuntime(j.getLogicJobName(), 600, 5400);
            if(j.getSubmitTime() <= (someTime) * Configuration.TIME_DIVISOR){
                Predictor.predictJobRuntime(j.getLogicJobName(), 600, 5400);
                CoalitionClient.sendJobRequest(new Job(j, true));
            }else{
                break;
            }

            //TODO Add Job usage, figure out rest of flow.
        }


    }
}
