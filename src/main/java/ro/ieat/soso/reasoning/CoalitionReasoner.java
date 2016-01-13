package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.predictor.persistence.MachineRepository;
import ro.ieat.soso.predictor.prediction.PredictionFactory;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;
import ro.ieat.soso.reasoning.controllers.persistence.CoalitionRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Created by adrian on 09.12.2015.
 * CoalitionReasoner is the name of the main component responsible for creation and future management of coalitions
 */
public class CoalitionReasoner {
    @Autowired
    private static CoalitionRepository coalitionRepository;

    public static Map<String, DurationPrediction> appDurationMap;
    public static final Double THRESHOLD = 0.4;
    private static Logger LOG = Logger.getLogger(CoalitionReasoner.class.toString());
    public static long c_id = 1;

    public static void initCoalitions(long time) throws Exception {


        Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();

        for(Machine m : MachineRepository.getInstance().findAll()){
            reason(m, coalitionMap, time);
        }

        for(Coalition c : coalitionMap.values()){
            sendCoalition(c);
        }
    }


    public static void sendCoalition(Coalition c) {

        if(c.getCurrentETA().getMax() == 0L) {
            for (Machine m : c.getMachines()) {
                if (m.getETA().getMax() > c.getCurrentETA().getMax()) {
                    c.setCurrentETA(m.getETA());
                }
            }
        }
        if(c.getId() == 0)
            c.setId(c_id++);
        coalitionRepository.save(c);
        LOG.info("Sending coalition " + c.getId() + "with size " + c.getMachines().size());

        CoalitionClient.sendCoalition(c);
    }

    public static void printCoaliion(Coalition c) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(c));
    }

    public static void reason(Machine machineProperties, Map<Long, Coalition> coalitionMap, long time) throws Exception {



        long minSize = Integer.MAX_VALUE;
        Coalition coalition = new Coalition();
        coalition.setCurrentETA(PredictionFactory.zeroDurationPrediction());

        //next lines are commented because I already took care of this during prediction of job times.
        long minJobRunTime = Long.MAX_VALUE;
        Map<Long, Job> currentJobs = MachineRepository.getInstance().jobRepo;
//        LOG.info("Tasks in machine : " + machineProperties.getTaskUsageList().size());
//        LOG.info("Current jobs  : " + currentJobs.size());

        for(TaskUsage taskUsage : machineProperties.getTaskUsageList()){
            Long jobId = taskUsage.getJobId();
            if(!currentJobs.containsKey(jobId))
                continue;
            //System.out.println(jobId + "\t" + machineProperties.getMachineId());
            long size = currentJobs.get(jobId).getTaskHistory().size();

//            LOG.info("Size in jobMap: " + size);
            if(size != 0 && size < minSize){
                minSize = size;
            }

            if(coalition.getJobs() == null){
                coalition.setJobs(new TreeMap<String, Long>());
            }

            String logJobName = currentJobs.get(jobId).getLogicJobName();

            if(appDurationMap.containsKey(logJobName)) {
                //long maxEndTime = currentJobs.get(jobId).getScheduleTime() + appDurationMap.get(logJobName).getMax();

                coalition.getJobs().put(logJobName, appDurationMap.get(logJobName).getMax());

                if (minJobRunTime > appDurationMap.get(logJobName).getMin()) {
                    coalition.setCurrentETA(appDurationMap.get(logJobName));
                    coalition.getCurrentETA().setMax(coalition.getCurrentETA().getMax() + currentJobs.get(jobId).getScheduleTime());
                    coalition.getCurrentETA().setMin(coalition.getCurrentETA().getMin() + currentJobs.get(jobId).getScheduleTime());
                    coalition.getCurrentETA().setAverage(coalition.getCurrentETA().getAverage() + currentJobs.get(jobId).getScheduleTime());
                    coalition.getCurrentETA().setHistogram(coalition.getCurrentETA().getHistogram() + currentJobs.get(jobId).getScheduleTime());
                    minJobRunTime = appDurationMap.get(logJobName).getMin();
                }
            }




        }

        if(MachineEventsMapper.MACHINES.containsKey(machineProperties.getId())) {
            machineProperties.setCpu(MachineEventsMapper.MACHINES.get(machineProperties.getId()).getKey());
            machineProperties.setMemory(MachineEventsMapper.MACHINES.get(machineProperties.getId()).getValue());
        }


        //Check availability of machine
        //TODO Here maybe getMaxCPU or something else would work better.
        Double availableCPU = machineProperties.getCpu() - machineProperties.getPrediction().getMaxCPU();
        Double availableMem = machineProperties.getMemory() - machineProperties.getPrediction().getMaxMemory();

        if(availableCPU > THRESHOLD * machineProperties.getCpu() && availableMem > THRESHOLD * machineProperties.getMemory()){

            //set machine as available from current time
            List<Long> available = new ArrayList<Long>();
            available.add(time * Configuration.TIME_DIVISOR);
            machineProperties.setETA(PredictionFactory.predictTime(available));
        }

//        LOG.info("min size: " + minSize);
//        LOG.info("machine usage size: " + machineProperties.getTaskUsageList().size());

        //machineProperties.setTaskUsageList(null);
        MachineRepository.getInstance().save(machineProperties);

        if(coalition.getMachines() == null)
            coalition.setMachines(new ArrayList<Machine>());
        coalition.getMachines().add(machineProperties);



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
                    coalition2.getMachines().add(machineProperties);
                }
            }
        }
    }


    public static int updateAll(long time){
        for(Coalition c : coalitionRepository.findAll()){
            reason(c, time);
            if(c.getCurrentETA().getMax() > (time + Configuration.STEP) * Configuration.TIME_DIVISOR)
                sendCoalition(c);
        }


        return 0;
    }



    public static int reason(Coalition coalition, long time) {

        for(Machine m : coalition.getMachines()){
            Machine mp = MachineRepository.getInstance().findOne(m.getId());

            long minSize = Long.MAX_VALUE;
            long minTaskStartTime = Long.MAX_VALUE;
            Map<Long, Job> currentJobs = MachineRepository.getInstance().jobRepo;
            for(TaskUsage taskUsage : mp.getTaskUsageList()){
                Long jobId = taskUsage.getJobId();
                if(!currentJobs.containsKey(jobId))
                    continue;

                long size = currentJobs.get(jobId).getTaskSize();
                if(size < minSize){
                    minSize = size;
                }
                if(coalition.getJobs() == null){
                    coalition.setJobs(new TreeMap<String, Long>());
                }

                String logJobName = currentJobs.get(jobId).getLogicJobName();

                if(appDurationMap.containsKey(logJobName)) {
                    //long maxEndTime = currentJobs.get(jobId).getScheduleTime() + appDurationMap.get(logJobName).getMax();

                    coalition.getJobs().put(logJobName, appDurationMap.get(logJobName).getMax());

                    if (minTaskStartTime > appDurationMap.get(logJobName).getMin()) {
                        coalition.setCurrentETA(appDurationMap.get(logJobName));
                        coalition.getCurrentETA().setMax(coalition.getCurrentETA().getMax() + currentJobs.get(jobId).getScheduleTime());
                        coalition.getCurrentETA().setMin(coalition.getCurrentETA().getMin() + currentJobs.get(jobId).getScheduleTime());
                        coalition.getCurrentETA().setAverage(coalition.getCurrentETA().getAverage() + currentJobs.get(jobId).getScheduleTime());
                        coalition.getCurrentETA().setHistogram(coalition.getCurrentETA().getHistogram() + currentJobs.get(jobId).getScheduleTime());
                        minTaskStartTime = appDurationMap.get(logJobName).getMin();
                    }
                }




            }
            m.setPrediction(mp.getPrediction());

            //Check availability of machine
            Double availableCPU = m.getCpu() - m.getPrediction().getMaxCPU();
            Double availableMem = m.getMemory() - m.getPrediction().getMaxMemory();

            if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){

                //set coalition as available from current time
                List<Long> available = new ArrayList<Long>();
                available.add(time);
                m.setETA(PredictionFactory.predictTime(available));
            }//The else case was treated in the previous 'for loop' to avoid the same computation
        }

        //TODO Each coalition is updated!!! So, send just the ones that are available from <time>



        //sendCoalition(coalition);
        return 0;
    }
}
