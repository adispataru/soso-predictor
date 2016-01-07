package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.core.mappers.MachineUsageMapper;
import ro.ieat.soso.core.prediction.Prediction;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by adrian on 09.12.2015.
 */
public class CoalitionResolutor {

    public static Map<Long, Job> JOBS;
    public static String OUTPUT_PATH;
    private static int c_id = 1;
    public static void resolute(Machine machineProperties, Map<Integer, List<Coalition>> coalitionMap) throws Exception {
        if(JOBS == null )
            throw new Exception("Jobs are not initialized");

        Machine machine = new Machine();
        machine.setId(machineProperties.getId());
        long maxJobId = 0;
        int maxSize = Integer.MAX_VALUE;
        Coalition coalition = new Coalition();
        List<Usage> usageList = new ArrayList<Usage>();
        long maxTaskRuntime = 0;
        long minTaskRuntime = Long.MAX_VALUE;

        for(TaskUsage taskUsage : machineProperties.getTaskUsageList()){
            Long jobId = taskUsage.getJobId();
            if(!JOBS.containsKey(jobId))
                continue;
            //System.out.println(jobId + "\t" + machineProperties.getMachineId());
            int size = JOBS.get(jobId).getTaskHistory().size();
            if(size < maxSize){
                maxSize = size;
                maxJobId = jobId;
            }
            if(coalition.getJobs() == null){
                coalition.setJobs(new TreeMap<String, Long>());
            }
            long runtime = JOBS.get(jobId).getFinishTime() - JOBS.get(jobId).getScheduleTime();
            if(runtime > 0)
                coalition.getJobs().put(JOBS.get(jobId).getLogicJobName(), runtime);

            for(TaskHistory th : JOBS.get(jobId).getTaskHistory()){
                if(th.getTaskIndex() == taskUsage.getTaskIndex()) {
                    if (maxTaskRuntime < th.getFinishTime() - th.getScheduleTime())
                        maxTaskRuntime = th.getFinishTime() - th.getScheduleTime();
                    if(minTaskRuntime > th.getFinishTime() - th.getScheduleTime() && th.getFinishTime() > th.getScheduleTime())
                        minTaskRuntime = th.getFinishTime() - th.getScheduleTime();
                }
            }

            for(Usage usage : taskUsage.getUsageList()){
                if(usageList.size() < taskUsage.getUsageList().size()){
                    usageList.add(usage);
                }else {
                    for (Usage anUsageList : usageList) {
                        if (anUsageList.getStartTime() != usage.getStartTime())
                            continue;

                        //TODO Do not add usage if usage time > job finish time
                        if(JOBS.get(jobId).getFinishTime() > 600 && usage.getStartTime() > JOBS.get(jobId).getFinishTime())
                            continue;
                        anUsageList.addCpu(usage.getCpu());
                        anUsageList.addMemory(usage.getMemory());
                        anUsageList.addMaxCpu(usage.getMaxCpu());
                        anUsageList.addMaxMemory(usage.getMaxMemory());

                    }
                }
            }


        }

//        coalitions.setLogicJobName(JOBS.get(maxJobId).getLogicJobName());
//        coalitions.setSchedule_class(JOBS.get(maxJobId).getScheduleClass());

        machine.setEstimatedCPULoad(Prediction.predictCPU(usageList));
        machine.setEstimatedMemoryLoad(Prediction.predictMemory(usageList));

        if(MachineEventsMapper.MACHINES.containsKey(machine.getId())) {
            machine.setCpu(MachineEventsMapper.MACHINES.get(machine.getId()).getKey());
            machine.setMemory(MachineEventsMapper.MACHINES.get(machine.getId()).getValue());
        }

        if(coalition.getCurrentETA() != null) {

            if (coalition.getCurrentETA().getMin() > minTaskRuntime)
                coalition.getCurrentETA().setMin(minTaskRuntime);
            if(coalition.getCurrentETA().getMax() < maxTaskRuntime)
                coalition.getCurrentETA().setMax(maxTaskRuntime);
        }else{
            List<Long> runtimes = new ArrayList<Long>();
            runtimes.add(minTaskRuntime);
            runtimes.add(maxTaskRuntime);
            coalition.setCurrentETA(Prediction.predictTime(runtimes));
        }


        if(coalition.getMachines() == null)
            coalition.setMachines(new ArrayList<Machine>());
        coalition.getMachines().add(machine);

        if (!coalitionMap.containsKey(maxSize)){
            coalitionMap.put(maxSize, new ArrayList<Coalition>());
            coalitionMap.get(maxSize).add(coalition);
        }else {
            //get last coalitions
            if(coalitionMap.get(maxSize).size() > 0) {
                Coalition coalition2 = coalitionMap.get(maxSize).get(coalitionMap.get(maxSize).size() - 1);
                if (coalition2.getMachines().size() == maxSize) {
                    coalitionMap.get(maxSize).add(coalition);
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

    public static void writeCoalitions(List<Coalition> coalitions) throws Exception {
        if(OUTPUT_PATH == null)
            throw new Exception("Output path not set for coalitions");

        ObjectMapper objectMapper = new ObjectMapper();
        for(Coalition c : coalitions){
            File dir = new File(OUTPUT_PATH);
            if(!dir.exists())
                dir.mkdirs();
            //TODO add features to coalitions
            FileWriter f = new FileWriter(OUTPUT_PATH + c_id);
            f.write(objectMapper.writeValueAsString(c));
            f.close();
            c_id += 1;
        }



    }

    public static void writeCoalition(Coalition coalition) throws Exception {
        if(OUTPUT_PATH == null)
            throw new Exception("Output path not set for coalitions");

        ObjectMapper objectMapper = new ObjectMapper();
            File dir = new File(OUTPUT_PATH);
        if(!dir.exists())
            dir.mkdirs();
        //TODO add features to coalitions

        FileWriter f;
        if(coalition.getId() != 0)
            f = new FileWriter(OUTPUT_PATH + coalition.getId());
        else
            f = new FileWriter(OUTPUT_PATH + c_id);
        f.write(objectMapper.writeValueAsString(coalition));
        f.close();
        c_id += 1;




    }

    public static void resolute(Coalition coalition, long start, long end) throws Exception {
        long maxTaskRuntime = 0;
        long minTaskRuntime = Long.MAX_VALUE;

        List<Long> runtimes = new ArrayList<Long>();
        for(Machine m : coalition.getMachines()){
            Machine mp = MachineUsageMapper.readOne(new File(Configuration.MACHINE_USAGE_PATH + m.getId()), start, end);
            List<Usage> usageList = new ArrayList<Usage>();
            for(TaskUsage taskUsage : mp.getTaskUsageList()){
                Long jobId = taskUsage.getJobId();
                if(!JOBS.containsKey(jobId))
                    continue;
                //System.out.println(jobId + "\t" + machineProperties.getMachineId());
                int size = JOBS.get(jobId).getTaskHistory().size();
                runtimes.add(JOBS.get(jobId).getFinishTime());

                for(TaskHistory th : JOBS.get(jobId).getTaskHistory()){
                    if(th.getTaskIndex() == taskUsage.getTaskIndex()) {
                        if (maxTaskRuntime < th.getFinishTime())
                            maxTaskRuntime = th.getFinishTime();
                        if(minTaskRuntime > th.getFinishTime())
                            minTaskRuntime = th.getFinishTime() - th.getScheduleTime();
                    }
                }

                for(Usage usage : taskUsage.getUsageList()){
                    if(usageList.size() < taskUsage.getUsageList().size()){
                        usageList.add(usage);
                    }else {
                        for (Usage anUsageList : usageList) {
                            if (anUsageList.getStartTime() != usage.getStartTime())
                                continue;

                            //TODO Do not add usage if usage time > job finish time
                            if(JOBS.get(jobId).getFinishTime() > 600 && usage.getStartTime() > JOBS.get(jobId).getFinishTime())
                                continue;
                            anUsageList.addCpu(usage.getCpu());
                            anUsageList.addMemory(usage.getMemory());
                            anUsageList.addMaxCpu(usage.getMaxCpu());
                            anUsageList.addMaxMemory(usage.getMaxMemory());

                        }
                    }
                }
            }
            m.setEstimatedCPULoad(Prediction.predictCPU(usageList));
            m.setEstimatedMemoryLoad(Prediction.predictMemory(usageList));
            //TODO insert prediction for runtime


        }

        if(coalition.getScheduledJobs() != null){

        }

        coalition.setCurrentETA(Prediction.predictTime(runtimes));


        writeCoalition(coalition);
    }
}
