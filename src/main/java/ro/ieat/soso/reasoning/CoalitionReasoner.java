package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.prediction.Prediction;
import ro.ieat.soso.predictor.prediction.PredictionFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by adrian on 09.12.2015.
 */
public class CoalitionReasoner {

    public static Map<Long, Machine> machinePredictionMap;
    public static Map<Long, Job> currentJobs;
    public static Map<String, Prediction<Long>> appDurationMap;
    public static final Double THRESHOLD = 0.01;

    public static void initCoalitions(long time) throws Exception {

        if(MachineEventsMapper.MACHINES.size() == 0)
            MachineEventsMapper.map(new FileReader(Configuration.MACHINE_EVENTS), 0L, time);



        Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();
        for(Machine m : machinePredictionMap.values()){
            resolute(m, coalitionMap, m.getEstimatedCPULoad().getStartTime());

        }
    }

    //TODO Send this coalition up via REST.
    public static void sendCoalition(Coalition c) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(c));
    }

    public static void resolute(Machine machineProperties, Map<Long, Coalition> coalitionMap, long time) throws Exception {

        Machine machine = new Machine();
        machine.setId(machineProperties.getId());
        long minSize = Integer.MAX_VALUE;
        Coalition coalition = new Coalition();

        //next lines are commented because I already took care of this during prediction of job times.
        long minTaskStartTime = Long.MAX_VALUE;
        for(TaskUsage taskUsage : machineProperties.getTaskUsageList()){
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

        if(MachineEventsMapper.MACHINES.containsKey(machine.getId())) {
            machine.setCpu(MachineEventsMapper.MACHINES.get(machine.getId()).getKey());
            machine.setMemory(MachineEventsMapper.MACHINES.get(machine.getId()).getValue());
        }

        //Check availability of machine
        Double availableCPU = machine.getCpu() - machine.getEstimatedCPULoad().getMax();
        Double availableMem = machine.getMemory() - machine.getEstimatedMemoryLoad().getMax();

        if(availableCPU > THRESHOLD * machine.getCpu() && availableMem > THRESHOLD * machine.getMemory()){

            //set coalition as available from current time
            List<Long> available = new ArrayList<Long>();
            available.add(time);
            coalition.setCurrentETA(PredictionFactory.predictTime(available));
        }//The else case was treated in the previous 'for loop' to avoid the same computation


        if(coalition.getMachines() == null)
            coalition.setMachines(new ArrayList<Machine>());
        coalition.getMachines().add(machine);

        if (!coalitionMap.containsKey(minSize)){
            coalitionMap.put(minSize, coalition);
        }else {
            //get last coalition and check if it has enough machines assigned, if yes, then add the created coalition,
            //otherwise add the Machine to the last coalition.
            if(coalitionMap.containsKey(minSize)) {
                Coalition coalition2 = coalitionMap.get(minSize);
                if (coalition2.getMachines().size() == minSize) {
                    sendCoalition(coalition2);
                    coalitionMap.put(minSize, coalition);
                }else{
                    if(coalition2.getJobs() == null)
                        coalition2.setJobs(new TreeMap<String, Long>());
                    if(coalition.getJobs() != null)
                        coalition2.getJobs().putAll(coalition.getJobs());
                    coalition2.getMachines().add(machine);
                }
            }
        }
    }




    public static void resolute(Coalition coalition, long time) throws Exception {

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
            m.setEstimatedCPULoad(mp.getEstimatedCPULoad());
            m.setEstimatedMemoryLoad(mp.getEstimatedMemoryLoad());

            //Check availability of machine
            Double availableCPU = m.getCpu() - m.getEstimatedCPULoad().getMax();
            Double availableMem = m.getMemory() - m.getEstimatedMemoryLoad().getMax();

            if(availableCPU > THRESHOLD * m.getCpu() && availableMem > THRESHOLD * m.getMemory()){

                //set coalition as available from current time
                List<Long> available = new ArrayList<Long>();
                available.add(time);
                coalition.setCurrentETA(PredictionFactory.predictTime(available));
            }//The else case was treated in the previous 'for loop' to avoid the same computation
        }

        if(coalition.getScheduledJobs() != null){
            //TODO Treat scheduled events.
        }



        sendCoalition(coalition);
    }
}
