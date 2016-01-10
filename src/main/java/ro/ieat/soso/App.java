package ro.ieat.soso;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.mappers.JobEventsMapper;
import ro.ieat.soso.core.mappers.TaskEventsMapper;
import ro.ieat.soso.core.mappers.TaskUsageMapper;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.predictor.Predictor;
import ro.ieat.soso.predictor.persistence.MachineRepository;
import ro.ieat.soso.reasoning.CoalitionReasoner;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
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

        configure(600, 6000);

        SpringApplication.run(App.class, args);


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
        MachineRepository.jobRepo = jobMap;

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

        CoalitionReasoner.currentJobs = jobMap;
        CoalitionReasoner.appDurationMap = new TreeMap<String, DurationPrediction>();
        for(Job j : jobMap.values()){
//            if(CoalitionReasoner.appDurationMap.containsKey(j.getLogicJobName()))
//                continue;
            Predictor.predictJobRuntime(j.getLogicJobName(), 600, 5400);
        }

        CoalitionReasoner.initCoalitions(5400);
        List<Coalition> cs = CoalitionReasoner.coalitionCollector;

        String coalitionOutputFolder = "./data/coalitions/";

        for(Coalition coalition : cs){
            System.out.printf("coalition id: %d; coalition size: %d\n", coalition.getId(), coalition.getMachines().size());
            ObjectMapper objectMapper = new ObjectMapper();
            File dir = new File(coalitionOutputFolder);
            if(!dir.exists())
                dir.mkdirs();
            //TODO add features to coalitions

            FileWriter f = new FileWriter(coalitionOutputFolder + coalition.getId());
            f.write(objectMapper.writeValueAsString(coalition));
            f.close();
        }

    }
}
