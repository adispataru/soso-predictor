package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.predictor.prediction.PredictionFactory;

import java.io.FileReader;
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

    public static Map<Long, Machine> machinePredictionMap;
    public static Map<Long, Job> currentJobs;
    public static Map<String, DurationPrediction> appDurationMap;
    public static final Double THRESHOLD = 0.01;
    private static Logger LOG = Logger.getLogger(CoalitionReasoner.class.toString());
    public static List<Coalition> coalitionCollector = new ArrayList<Coalition>();
    public static long c_id = 1;

    public static void initCoalitions(long time) throws Exception {

        if(MachineEventsMapper.MACHINES.size() == 0)
            MachineEventsMapper.map(new FileReader(Configuration.MACHINE_EVENTS), 0L, time);



        Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();

        for(Machine m : machinePredictionMap.values()){
            update(m, coalitionMap, m.getPrediction().getStartTime());
        }

        for(Coalition c : coalitionMap.values()){
            c.setId(c_id++);
            sendCoalition(c);
        }
    }


    public static void sendCoalition(Coalition c) throws IOException {
        if (c.getCurrentETA() == null)
            c.setCurrentETA(PredictionFactory.maxLongDurationPrediction());

        coalitionCollector.add(c);
    }

    public static void printCoaliion(Coalition c) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(c));
    }

    public static void update(Machine machineProperties, Map<Long, Coalition> coalitionMap, long time) throws Exception {



        long minSize = Integer.MAX_VALUE;
        Coalition coalition = new Coalition();

        //next lines are commented because I already took care of this during prediction of job times.
        long minJobRunTime = Long.MAX_VALUE;
        for(TaskUsage taskUsage : machineProperties.getTaskUsageList()){
            Long jobId = taskUsage.getJobId();
            if(!currentJobs.containsKey(jobId))
                continue;
            //System.out.println(jobId + "\t" + machineProperties.getMachineId());
            long size = currentJobs.get(jobId).getTaskSize();
            if(size == 0)
                size = currentJobs.get(jobId).getTaskHistory().size();
            if(size != 0 && size < minSize){
                minSize = size;
            }
            if(coalition.getJobs() == null){
                coalition.setJobs(new TreeMap<String, Long>());
            }

            String logJobName = currentJobs.get(jobId).getLogicJobName();

            //TODO For now I take the max, discuss alternatives
            //TODO Fix: This block may create null currentETA. Avoid this to happen.
            if(appDurationMap.containsKey(logJobName)) {
                long maxEndTime = currentJobs.get(jobId).getScheduleTime() + appDurationMap.get(logJobName).getMax();

                coalition.getJobs().put(logJobName, maxEndTime);

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
        Double availableCPU = machineProperties.getCpu() - machineProperties.getPrediction().getMaxCPU();
        Double availableMem = machineProperties.getMemory() - machineProperties.getPrediction().getMaxMemory();

        if(availableCPU > THRESHOLD * machineProperties.getCpu() && availableMem > THRESHOLD * machineProperties.getMemory()){

            //set coalition as available from current time
            List<Long> available = new ArrayList<Long>();
            available.add(time);
            coalition.setCurrentETA(PredictionFactory.predictTime(available));
        }//The else case was treated in the previous 'for loop' to avoid the same computation


        //TODO Check if this doesn't create problems
        machineProperties.setTaskUsageList(null);

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




    public static int update(Coalition coalition, long time) throws Exception {

        for(Machine m : coalition.getMachines()){
            Machine mp = machinePredictionMap.get(m.getId());

            long minSize = Long.MAX_VALUE;
            long minTaskStartTime = Long.MAX_VALUE;
            for(TaskUsage taskUsage : mp.getTaskUsageList()){
                Long jobId = taskUsage.getJobId();
                if(!currentJobs.containsKey(jobId))
                    continue;
                //System.out.println(jobId + "\t" + machineProperties.getMachineId());
                long size = currentJobs.get(jobId).getTaskSize();
                if(size < minSize){
                    minSize = size;
                }
                if(coalition.getJobs() == null){
                    coalition.setJobs(new TreeMap<String, Long>());
                }

                String logJobName = currentJobs.get(jobId).getLogicJobName();

                //TODO For now I take the max, discuss alternatives
                //TODO For now I take the max, discuss alternatives
                if(appDurationMap.containsKey(logJobName)) {
                    long maxEndTime = currentJobs.get(jobId).getScheduleTime() + appDurationMap.get(logJobName).getMax();

                    coalition.getJobs().put(logJobName, maxEndTime);

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
            System.out.printf("ECPU-EndTime: %d", mp.getPrediction().getEndTime());
            m.setPrediction(mp.getPrediction());

            //Check availability of machine
            Double availableCPU = m.getCpu() - m.getPrediction().getMaxCPU();
            Double availableMem = m.getMemory() - m.getPrediction().getMaxMemory();

            if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){

                //set coalition as available from current time
                List<Long> available = new ArrayList<Long>();
                available.add(time);
                coalition.setCurrentETA(PredictionFactory.predictTime(available));
            }//The else case was treated in the previous 'for loop' to avoid the same computation
        }

        //TODO: Treat scheduled jobs
//        if(coalition.getScheduledJobs() != null){
//
//        }



        //sendCoalition(coalition);
        return 0;
    }
}
