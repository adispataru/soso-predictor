package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.persistence.CoalitionRepository;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.predictor.prediction.PredictionFactory;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by adrian on 09.12.2015.
 * CoalitionReasoner is the name of the main component responsible for creation and future management of coalitions
 */

@RestController
public class CoalitionReasoner {

    public static Map<String, DurationPrediction> appDurationMap;
    public static final Double THRESHOLD = 0.4;
    private static Logger LOG = Logger.getLogger(CoalitionReasoner.class.toString());
    public static long c_id = 1;
    private CoalitionClient coalitionClient = new CoalitionClient();

    @Autowired
    CoalitionRepository coalitionRepository;
    @Autowired
    MachineRepository machineRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;



    @RequestMapping(method = RequestMethod.GET, path = "/coalitions/init/{time}")
    public Integer initCoalitions(@PathVariable long time) throws Exception {


        Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();

        int i = 1;
        long size = machineRepository.count();

        for(Machine m : machineRepository.findAll()) {
            reason(m, coalitionMap, time);
//            LOG.info("Machine number: " + i);
            if(i % size / 10 == 0)
                LOG.info(String.format("%.2f%%", i * 1.0 / size));
            i++;
        }

        LOG.info("Coalitions created: " + coalitionMap.size());
        for(Coalition c : coalitionMap.values()){
            sendCoalition(c);
        }
        return coalitionMap.size();
    }


    public void sendCoalition(Coalition c) {

//        if(c.getCurrentETA().getMax() == 0L) {
//            for (Long mID : c.getMachines()) {
//                Machine m = machineRepository.findOne(mID);
//                if (m.getETA().getMax() > c.getCurrentETA().getMax()) {
//                    c.setCurrentETA(m.getETA());
//                }
//            }
//        }

        if(c.getId() == 0)
            c.setId(c_id++);

        LOG.info("Sending coalition " + c.getId() + "with size " + c.getMachines().size());

        coalitionClient.sendCoalition(c);
    }

    public static void printCoaliion(Coalition c) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(c));
    }

    public void reason(Machine machineProperties, Map<Long, Coalition> coalitionMap, long time) throws Exception {



        long minSize;
        Coalition coalition = new Coalition();
        coalition.setCurrentETA(PredictionFactory.zeroDurationPrediction());
        coalition.setConfidenceLevel(.0);

        if(MachineEventsMapper.MACHINES.containsKey(machineProperties.getId())) {
            machineProperties.setCpu(MachineEventsMapper.MACHINES.get(machineProperties.getId()).getKey());
            machineProperties.setMemory(MachineEventsMapper.MACHINES.get(machineProperties.getId()).getValue());
        }


        //Check availability of machine
        //DONE Here maybe getMaxCPU or something else would work better.
        Double availableCPU = machineProperties.getCpu() - machineProperties.getPrediction().getMaxCPU();
        Double availableMem = machineProperties.getMemory() - machineProperties.getPrediction().getMaxMemory();

        if(availableCPU > THRESHOLD * machineProperties.getCpu() && availableMem > THRESHOLD * machineProperties.getMemory()){

            //set machine as available from current time
            List<Long> available = new ArrayList<Long>();
            available.add(time * Configuration.TIME_DIVISOR);
            machineProperties.setETA(PredictionFactory.predictTime(available));
            machineRepository.save(machineProperties);
            if (machineProperties.getETA().getMax() > coalition.getCurrentETA().getMax()) {
                coalition.setCurrentETA(machineProperties.getETA());
            }

        }

//        LOG.info("min size: " + minSize);
//        LOG.info("machine usage size: " + machineProperties.getTaskUsageList().size());

        //machineProperties.setTaskUsageList(null);

        if(coalition.getMachines() == null)
            coalition.setMachines(new ArrayList<Long>());
        coalition.getMachines().add(machineProperties.getId());



//        LOG.info("Min size for coalition " + minSize);

        minSize = new Random(System.currentTimeMillis()).nextInt(100);
        if (!coalitionMap.containsKey(minSize)){
            coalitionMap.put(minSize, coalition);
        }else {
            //get last coalition and check if it has enough machines assigned, if yes, then add the created coalition,
            //otherwise add the Machine to the last coalition.
            if(coalitionMap.containsKey(minSize)) {
                Coalition coalition2 = coalitionMap.get(minSize);
                if (coalition2.getMachines().size() == minSize) {
                    coalition2.setId(c_id++);
                    sendCoalition(coalition2);
                    coalitionMap.put(minSize, coalition);
                }else{
                    if(coalition2.getJobs() == null)
                        coalition2.setJobs(new TreeMap<String, Long>());
                    if(coalition.getJobs() != null)
                        coalition2.getJobs().putAll(coalition.getJobs());
                    coalition2.getMachines().add(machineProperties.getId());
                }
            }
        }
    }


    @RequestMapping(method = RequestMethod.PUT, path = "/coalitions/update")
    public void updateAll(long time){
        for(Coalition c : coalitionRepository.findAll()){
            reason(c, time);
            c = coalitionRepository.findOne(c.getId());
            if(c.getCurrentETA().getMax() > (time + Configuration.STEP) * Configuration.TIME_DIVISOR)
                sendCoalition(c);
        }

    }



    public int reason(Coalition coalition, long time) {

        for(Long m : coalition.getMachines()){
            Machine mp = machineRepository.findOne(m);

            long minSize = Long.MAX_VALUE;
            long minTaskStartTime = Long.MAX_VALUE;
            List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                    findByMachineIdAndStartTimeLessThan(mp.getId(), time);
            for(TaskUsage taskUsage : taskUsageList){
                //Get list before to avoid repeated interogation
//            TaskUsage taskUsage = taskUsageMappingRepository.findOne(taskUsageId);
                Long jobId = taskUsage.getJobId();
                Job job = jobRepository.findOne(jobId);
                if(job == null)
                    continue;

                long size = job.getTaskSize();
                if(size < minSize){
                    minSize = size;
                }
                if(coalition.getJobs() == null){
                    coalition.setJobs(new TreeMap<String, Long>());
                }

                String logJobName = job.getLogicJobName();

                if(appDurationMap.containsKey(logJobName)) {
                    //long maxEndTime = currentJobs.get(jobId).getScheduleTime() + appDurationMap.get(logJobName).getMax();

                    coalition.getJobs().put(logJobName, appDurationMap.get(logJobName).getMax());

                    if (minTaskStartTime > appDurationMap.get(logJobName).getMin()) {
                        coalition.setCurrentETA(appDurationMap.get(logJobName));
                        coalition.getCurrentETA().setMax(coalition.getCurrentETA().getMax() + job.getScheduleTime());
                        coalition.getCurrentETA().setMin(coalition.getCurrentETA().getMin() + job.getScheduleTime());
                        coalition.getCurrentETA().setAverage(coalition.getCurrentETA().getAverage() + job.getScheduleTime());
                        coalition.getCurrentETA().setHistogram(coalition.getCurrentETA().getHistogram() + job.getScheduleTime());
                        minTaskStartTime = appDurationMap.get(logJobName).getMin();
                    }
                }




            }
            mp.setPrediction(mp.getPrediction());

            //Check availability of machine
            Double availableCPU = mp.getCpu() - mp.getPrediction().getMaxCPU();
            Double availableMem = mp.getMemory() - mp.getPrediction().getMaxMemory();

            if(availableCPU > THRESHOLD * mp.getCpu() && availableMem > THRESHOLD * mp.getMemory()){

                //set coalition as available from current time
                List<Long> available = new ArrayList<Long>();
                available.add(time);
                mp.setETA(PredictionFactory.predictTime(available));
                machineRepository.save(mp);
                if (mp.getETA().getMax() > coalition.getCurrentETA().getMax()) {
                    coalition.setCurrentETA(mp.getETA());
                }
            }//The else case was treated in the previous 'for loop' to avoid the same computation
        }

        //TODO Each coalition is updated!!! So, send just the ones that are available from <time>

        coalitionRepository.save(coalition);


        //sendCoalition(coalition);
        return 0;
    }
}
