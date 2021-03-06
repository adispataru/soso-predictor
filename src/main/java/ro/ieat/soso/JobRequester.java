package ro.ieat.soso;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.JobEventsMapper;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.mappers.TaskEventsMapper;
import ro.ieat.soso.core.mappers.TaskUsageMapper;
import ro.ieat.soso.core.prediction.PredictionMethod;
import ro.ieat.soso.persistence.*;
import ro.ieat.soso.predictor.prediction.PredictionFactory;
import ro.ieat.soso.predictor.prediction.duration.PessimisticDurationPrediction;
import ro.ieat.soso.predictor.prediction.taskusage.PessimisticTaskUsagePrediction;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;


/**
 * Created by adrian on 14.01.2016.
 *
 * This class represents the workflow of our experiment
 */

@RestController
public class JobRequester {
    private static final Logger LOG = Logger.getLogger("JobRequester");
    private CoalitionClient coalitionClient = new CoalitionClient();
    private static long taskUsageCounter = 0;
    RestTemplate template;
    @Autowired
    MachineRepository machineRepository;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;

    @Autowired
    CoalitionRepository coalitionRepository;

    @Autowired
    ScheduledRepository scheduledRepository;
    String[] jobRequestTargetUrls = {"http://localhost:8090/job", "http://localhost:8091/job"};
    public static String[] types = {"rb-tree", "linear"};



    @RequestMapping(method = RequestMethod.GET, path = "/app/init/{start}/{end}")
    public String populateDB(@PathVariable("start") long initStart,@PathVariable("end") long initEnd) throws IOException, InterruptedException {
        //        Configuration.JOB_EVENTS_PATH = "/home/adrian/work/ieat/CloudLightning/soso-predictor/" + Configuration.JOB_EVENTS_PATH;
//        Configuration.TASK_EVENTS_PATH = "/home/adrian/work/ieat/CloudLightning/soso-predictor/" + Configuration.TASK_EVENTS_PATH;
//        Configuration.TASK_USAGE_PATH = "/home/adrian/work/ieat/CloudLightning/soso-predictor/" + Configuration.TASK_USAGE_PATH;
//        Configuration.MACHINE_EVENTS = "/home/adrian/work/ieat/CloudLightning/soso-predictor/" + Configuration.MACHINE_EVENTS;

        Long maxTime = Long.MAX_VALUE / Configuration.TIME_DIVISOR;

        long time = System.currentTimeMillis();


        if(machineRepository.count() == 0) {
            //Make use of machine_events file to populate RepositoryPool;
            LOG.info("Starting machine mapping");
            MachineEventsMapper.map(new FileReader(Configuration.MACHINE_EVENTS), 0, maxTime);
            LOG.info("Done.");


            LOG.info("Generating machines based on events file");
            for (Long id : MachineEventsMapper.MACHINES.keySet()) {
                Machine m = new Machine(id, MachineEventsMapper.MACHINES.get(id).getKey(),
                        MachineEventsMapper.MACHINES.get(id).getValue());
                machineRepository.save(m);
            }
        }
        else{
            LOG.info("Machines data exists in MongoDB");
        }
        if(jobRepository.count() == 0) {

            Map<Long, Job> jobMap = new TreeMap<Long, Job>();
            for(int i = 0 ; i < 4; i++){
                String jobEvents = Configuration.JOB_EVENTS_PATH + String.format("part-%05d-of-00500.csv", i);
                String taskEvents = Configuration.TASK_EVENTS_PATH + String.format("part-%05d-of-00500.csv", i);
                String taskUsages = Configuration.TASK_USAGE_PATH + String.format("part-%05d-of-00500.csv", i);
                File jef = new File(jobEvents);
                File tef = new File(taskEvents);
                File tuf = new File(taskUsages);
                if(!jef.exists() || !tef.exists() || !tuf.exists())
                    break;
                LOG.info("Reading from part " + i);
                LOG.info("Jobs...");
                JobEventsMapper.map(new FileReader(jef), jobMap, initStart, maxTime);
                LOG.info("Tasks...");
                TaskEventsMapper.map(new FileReader(tef), jobMap, initStart, maxTime);
                LOG.info("Tasks usage...");
                List<TaskUsage> taskUsageList = TaskUsageMapper.map(new FileReader(tuf), initStart, initEnd);
                LOG.info("Done.");
                LOG.info("Task usage records: " + taskUsageList.size());
                LOG.info("First task time: " + taskUsageList.get(0).getEndTime());
                LOG.info("Last task time: " + taskUsageList.get(taskUsageList.size() -1).getEndTime());

                for(int j = 0; j < 15; j++){
                    taskUsageMappingRepository.save(taskUsageList.subList(j * (taskUsageList.size()/15), (j+1) * taskUsageList.size() / 15));
                }


                LOG.info("Saving and cleaning up finished jobs...");
                Iterator<Map.Entry<Long, Job>> iterator = jobMap.entrySet().iterator();
                while(iterator.hasNext()){
                    Job j = iterator.next().getValue();

                    if(j.getTaskHistory().size() < 12500) {
//                        LOG.info("Saving job " + j.getJobId());
                        jobRepository.save(new Job(j, false));
                    }else{
                        LOG.severe("Not saving job " + j.getJobId() + " because it has " + j.getTaskHistory().size() + " tasks.");
                        //iterator.remove();
                    }
                    if(j.getStatus().equals("finish") || j.getStatus().equals("kill") || j.getStatus().equals("fail"))
                        iterator.remove();
                }
            }
        }else {
            LOG.info("Jobs data existent in mongo.");
            //Not needed anymore
//            LOG.info("Setting assigned machine id to 0. ");
//            for(TaskUsage usage : taskUsageMappingRepository.findAll().stream().
//                    filter(t -> t.getAssignedMachineId() != 0).collect(Collectors.toList())){
//                usage.setAssignedMachineId(0L);
//                taskUsageMappingRepository.save(usage);
//            }
            LOG.info("Done. ");
        }
        return "Done.";

    }


    @RequestMapping(method = RequestMethod.PUT, path = "/app/start/{startTime}/{endTime}/{historySize}")
    public void jobRequestFlow(@PathVariable long startTime, @PathVariable long endTime, @PathVariable int historySize) throws Exception{


        coalitionRepository.deleteAll();
        scheduledRepository.deleteAll();
        long scheduledId = 0;


        long initStart = startTime - Configuration.STEP * historySize, initEnd = startTime;
        PredictionMethod machinePredictionMethod = new PessimisticTaskUsagePrediction();
        PredictionMethod jobPredictionMethod = new PessimisticDurationPrediction();
        PredictionFactory.setPredictionMethod("machine", machinePredictionMethod);
        PredictionFactory.setPredictionMethod("job", jobPredictionMethod);

        template = new RestTemplate();
        long time = System.currentTimeMillis();
        LOG.info("Predicting machine usage...");
        String predictionPath = "http://localhost:8088/predict/allUsage/" + initStart + "/" + initEnd;
        //predict taskUsage for all machines
//        template.put(predictionPath, 1);

        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));


        //TODO This will be added in future versions
//        time = System.currentTimeMillis();
//        LOG.info("Predicting job runtime");
//        template.put("http://localhost:8088/predict/job/0/" + initEnd, 1);
//        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        LOG.info("Initializing coalitions...");
        Integer csize = template.getForObject("http://localhost:8088/coalitions/init/" + initEnd, Integer.class);
        LOG.info("Initialized " + csize + " coalitions");
//        for(Coalition c : coalitionRepository.findAll()){
//            LOG.info("Id: " + c.getId());
//        }



        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));


        time = initEnd + Configuration.STEP;
        boolean updateCoalition = false;
        while(time <= endTime) {
            Map<String, Long> notScheduledTasks = new TreeMap<>();
            Map<String, Long> notScheduledJobs = new TreeMap<>();
            for(String type : JobRequester.types) {
                notScheduledTasks.put(type, 0L);
                notScheduledJobs.put(type, 0L);
            }
            if(updateCoalition){
//                LOG.info("Predicting machine usage...");
//                predictionPath = "http://localhost:8088/predict/allUsage/" + initStart + "/" + initEnd;
//                template.put(predictionPath, 1);
                LOG.info("Updating coalitions");
                template.put("http://localhost:8088/coalitions/update/" + initEnd, 1);
                LOG.info("Done.");

            }

            LOG.info("Searching jobs between " + initEnd + " and " + time);
            List<Job> jobs = jobRepository.findBySubmitTimeBetween(
                    initEnd * Configuration.TIME_DIVISOR, time * Configuration.TIME_DIVISOR);


            LOG.info(String.format("Found %d jobs", jobs.size()));
            long total = 0;
            for (Job j : jobs) {

                //LOG.info("For job " + j.getJobId() + " status is " + j.getStatus() + " at time " + j.getSubmitTime());
                total += j.getTaskHistory().size();

                if (j.getStatus() != null) {


                    //Send job request to main matcher

                    int i = 0;
                    for(String type : types) {
                        ScheduledJob scheduledJob = coalitionClient.sendJobRequest(new Job(j, false), jobRequestTargetUrls[i]);
                        if (scheduledJob != null) {
                            LOG.info("Scheduled job " + scheduledJob.getJobId());
                            scheduledJob.setId(scheduledId++);
                            scheduledJob.setScheduleType(type);
                            scheduledRepository.save(scheduledJob);
                        } else {
                            notScheduledTasks.put(type, notScheduledTasks.get(type) + j.getTaskHistory().size());
                            notScheduledJobs.put(type, notScheduledJobs.get(type) + 1);
                            LOG.severe(String.format("Job %d cannot be scheduled by %s", j.getJobId(), type));
                        }
                        i++;
                    }


                } else {
                    LOG.info("Not sending " + j.getJobId() + " because status is " + j.getStatus() + " at time " + j.getSubmitTime());
                }
            }
            int sent = jobs.size();
            writeJobSchedulingErrors(notScheduledTasks, notScheduledJobs, sent, total, time);


            initEnd = time;
            time += Configuration.STEP;
            template = new RestTemplate();
            if (time > endTime){
                Job endJob = new Job();
                endJob.setJobId(-1);
                LOG.info("Sending end job");
                for(String jobUrl : jobRequestTargetUrls) {
                    coalitionClient.sendJobRequest(endJob, jobUrl);
                }
            }
            template.put("http://localhost:8088/finetuner/" + initEnd, 1);
            LOG.info("Predicting job runtime");
            template.put("http://localhost:8088/predict/job/" + initEnd +"/" + time, 1);
            LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));
//            updateCoalition = true;
        }






        LOG.exiting("experiment", "JobRequester");

        LOG.info("Success!");


    }

    //TODO Write total jobs
    public void writeJobSchedulingErrors(Map<String, Long> notScheduledTasks, Map<String, Long> notScheduledJobs, long sent, long total, long time){
        try {
            File f = new File("./output/results/schedule/not_planned_errors");
            if(!f.exists()){
                FileWriter header = new FileWriter(f);
                header.write("#time rb-treeJobs tasks linearJobs tasks randomJobs tasks sentJobs sentTasks");
                header.close();
            }
            FileWriter fileWriter = new FileWriter(f, true);
            fileWriter.write(time + " ");
            for (String s : JobRequester.types) {
                fileWriter.write(notScheduledJobs.get(s) + " " + notScheduledTasks.get(s) + " ");

            }
            fileWriter.write(sent + " " + total + "\n");
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
