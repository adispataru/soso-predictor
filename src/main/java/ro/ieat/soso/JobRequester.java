package ro.ieat.soso;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
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
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.evaluator.Evaluator;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.reasoning.CoalitionReasoner;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;
import ro.ieat.soso.util.TaskUsageConqueror;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;


/**
 * Created by adrian on 14.01.2016.
 */

@RestController
public class JobRequester {
    private static final Logger LOG = Logger.getLogger("JobRequester");
    private CoalitionClient coalitionClient = new CoalitionClient();
    private static long taskUsageCounter = 0;
    RestTemplate template = new RestTemplate();
    @Autowired
    MachineRepository machineRepository;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;



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
            for(int i = 0 ; i < 2; i++){
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
                JobEventsMapper.map(new FileReader(jef), jobMap, initStart, initEnd);
                LOG.info("Tasks...");
                TaskEventsMapper.map(new FileReader(tef), jobMap, initStart, initEnd);
                LOG.info("Tasks usage...");
                TaskUsageConqueror.map(new FileReader(tuf), jobMap, initStart, initEnd);
                LOG.info("Done.");

                LOG.info("Cleaning up finished jobs");
                Iterator<Map.Entry<Long, Job>> iterator = jobMap.entrySet().iterator();
                while(iterator.hasNext()){
                    Job j = iterator.next().getValue();

                    if(j.getTaskHistory().size() < 1000) {
//                        LOG.info("Saving job " + j.getJobId());
                        jobRepository.save(new Job(j, false));
                        for(TaskHistory th : j.getTaskHistory().values()){
                            String postMachineUsage = "http://localhost:8088/assign/usage/" + th.getMachineId();
                            HttpHeaders headers = new HttpHeaders();
                            headers.setContentType(MediaType.APPLICATION_JSON);
                            if(th.getTaskUsage() != null) {
                                TaskUsage t = th.getTaskUsage();
                                t.setId(taskUsageCounter++);
                                taskUsageMappingRepository.save(t);
                                Machine m = machineRepository.findOne(th.getMachineId());
                                m.getTaskUsageList().add(t.getId());
                                machineRepository.save(m);
                            }else{
                                break;
                            }
                        }
                    }else{
                        LOG.severe("Not saving job " + j.getJobId() + " because it has " + j.getTaskHistory().size() + " tasks.");
                        iterator.remove();
                    }
                    if(j.getStatus().equals("finish"))
                        iterator.remove();
                }
            }

//            for (File f : new File(Configuration.JOB_EVENTS_PATH).listFiles()) {
//                JobEventsMapper.map(new FileReader(f), jobMap, initStart, initEnd);
//            }
//
//            LOG.info("Finished job events mapping");
//            for (File f : new File(Configuration.TASK_EVENTS_PATH).listFiles()) {
//                TaskEventsMapper.map(new FileReader(f), jobMap, initStart, initEnd);
//            }
//
//            LOG.info("Finished task events mapping");
//
//            jobMap = MapsUtil.sortJobMaponSubmitTime(jobMap);
//
//            Iterator<Map.Entry<Long, Job>> iterator = jobMap.entrySet().iterator();
//            while (iterator.hasNext()) {
//                Job j = iterator.next().getValue();
//                template.postForObject("http://localhost:8088/jobs", j, Job.class);
//            }
//
//
//            LOG.info("Starting task usage mapping");
//            File dir = new File(Configuration.TASK_USAGE_PATH);
//            if (dir.isDirectory()) {
//                int i = 1;
//                for (File f : dir.listFiles()) {
//                    TaskUsageConqueror.map(new FileReader(f), jobMap, initStart, initEnd);
//                    LOG.info("Processed " + i + " of " + dir.listFiles().length + " files...");
//                    ++i;
//                }
//            } else {
//                TaskUsageMapper.map(new FileReader(Configuration.TASK_USAGE_PATH), jobMap, initStart, initEnd);
//            }
//
//            LOG.info("Done.");
//
//            long time2 = System.currentTimeMillis();
//            LOG.info("Reading duration: " + (time2 - time));
        }else {
            LOG.info("Jobs data existent in mongo.");
        }
        return "Done.";

    }


    @RequestMapping(method = RequestMethod.PUT, path = "/app/start")
    public void jobRequestFlow() throws Exception{

        coalitionClient.deleteCoalitionsFromRepository();

        long initStart = 3600, initEnd = 5700;

        CoalitionReasoner.appDurationMap = new TreeMap<String, DurationPrediction>();


        long time = System.currentTimeMillis();
        LOG.info("Predicting machine usage...");
        String predictionPath = "http://localhost:8088/predict/allUsage/" + initStart + "/" + initEnd;
        template.put(predictionPath, 1);
//        for(Machine m : machineRepository.findAll()){
//            //MachinePrediction mp = Predictor.predictMachineUsage(m, initStart, initEnd);
//
////            LOG.info("prediction path " + predictionPath);
//            MachinePrediction mp = ;
//            m.setPrediction(mp);
//            machineRepository.save(m);
//        }

        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));

        time = System.currentTimeMillis();
        LOG.info("Initializing coalitions...");
        Integer csize = template.getForObject("http://localhost:8088/coalitions/init/" + initEnd, Integer.class);
        LOG.info("Initialized " + csize + " coalitions");
//        for(Coalition c : coalitionRepository.findAll()){
//            LOG.info("Id: " + c.getId());
//        }



        LOG.info(String.format("Done in %d ms.", System.currentTimeMillis() - time));


//        RepositoryPool.getInstance().jobRepo = MapsUtil.sortJobMaponSubmitTime(jobMap);

//        if(RepositoryPool.getInstance().jobRepo == null)
//            RepositoryPool.getInstance().jobRepo = new TreeMap<Long, Job>();
//        while (iterator.hasNext()){
//            Job j = iterator.next().getValue();
//            if(j.getSubmitTime() == 0)
//                continue;
//            RepositoryPool.getInstance().jobRepo.put(j.getJobId(), j);
//            Predictor.predictJobRuntime(j.getLogicJobName(), initStart, initEnd-300);
//            if(j.getSubmitTime() >= (initEnd) * Configuration.TIME_DIVISOR){
//                coalitionClient.sendJobRequest(new Job(j, true));
//                LOG.info("For job " + j.getJobId() + " status is " + j.getStatus() + " at time " + j.getSubmitTime());
//                break;
//            }
//
//        }


        time = initEnd + Configuration.STEP;
        long experimentEndTime = 7000;
        while(time <= experimentEndTime) {
            LOG.info("Searching jobs between " + initEnd + " and " + time);
            Iterator<Job> iterator = jobRepository.findBySubmitTimeBetween(
                    initEnd * Configuration.TIME_DIVISOR, time * Configuration.TIME_DIVISOR).iterator();

            while(iterator.hasNext()) {
                Job j = iterator.next();
                LOG.info("For job " + j.getJobId() + " status is " + j.getStatus() + " at time " + j.getSubmitTime());

                if (j.getTaskHistory().get(0L).getTaskUsage() == null)
                    continue;

                if (j.getStatus().equals("finish")) {
                    if (j.getSubmitTime() <= (time) * Configuration.TIME_DIVISOR) {
                        //Predictor.predictJobRuntime(j.getLogicJobName(), initStart, time);
                        ScheduledJob scheduledJob = coalitionClient.sendJobRequest(new Job(j, true));
                        if (scheduledJob != null) {
                            Evaluator.evaluate(scheduledJob);
                        } else {

                            LOG.severe(String.format("Job %d cannot be scheduled", j.getJobId()));
                        }
                    } else {
                        template.put("http://localhost:8088/coalitions/update", null);
                        //TODO Mechanism to predict.
                        //Predictor.predictJobRuntime(j.getLogicJobName(), initStart, time);
                        ScheduledJob scheduledJob = coalitionClient.sendJobRequest(new Job(j, true));
                        if (scheduledJob != null) {
                            Evaluator.evaluate(scheduledJob);
                        } else {

                            LOG.severe(String.format("Job %d cannot be scheduled", j.getJobId()));
                        }

                        if (experimentEndTime - time > Configuration.STEP) {
                            time += Configuration.STEP;
                        } else {
                            break;
                        }
                    }
                } else {
                    //TODO Log this to machine usage...
                    LOG.info("Not sending " + j.getJobId() + " because status is " + j.getStatus() + " at time " + j.getSubmitTime());
                }
            }

            initEnd = time;
            time += Configuration.STEP;
        }


    }
}
