package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import ro.ieat.soso.App;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.persistence.*;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;
import ro.ieat.soso.reasoning.startegies.AntColonyClusteringStrategy;
import ro.ieat.soso.reasoning.startegies.RankAndLabelCoalitionStrategy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static ro.ieat.soso.JobRequester.types;
import static ro.ieat.soso.evaluator.FineTuner.testOutputPath;

/**
 * Created by adrian on 09.12.2015.
 * CoalitionReasoner is the name of the main component responsible for creation and future management of coalitions
 */

@RestController
public class CoalitionReasoner {

    public static final Double THRESHOLD = 0.4;
    private static Logger LOG = Logger.getLogger(CoalitionReasoner.class.toString());
    public static long c_id = 1;
    private CoalitionClient coalitionClient = new CoalitionClient();
    private int coalitionStrategyId = 0; // 0 - ACC; 1 - RaL; 2 - Random

    @Autowired
    CoalitionRepository coalitionRepository;
    @Autowired
    MachineRepository machineRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    ScheduledRepository scheduledRepository;


    @RequestMapping(method = RequestMethod.GET, path = "/coalitions/init/{time}")
    public Integer initCoalitions(@PathVariable long time) throws Exception {


        Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();

        int i = 1;

        List<Coalition> coalitions = createCoalitions(machineRepository.findAll(), time, coalitionStrategyId);


        for (Coalition c : coalitions) {
            for(int type = 0; type < types.length; type++){
                c.setScheduleClass(type);
                c.setId(c_id++);
                coalitionRepository.save(c);
                sendCoalitionToComponent(c, time, type);
            }
        }
//        coalitionRepository.save(coalitions);
        int size = coalitions.size();
        LOG.info("Coalitions created: " + size);
        int[] total = new int[]{size, size, size};
        writeResults(time, new int[]{0, 0, 0}, total, total);
        return coalitions.size();
    }


    public void sendCoalition(Coalition c, long time) {

        if(time / 10000 < 10)
            time = time * Configuration.TIME_DIVISOR;
        if(c.getCurrentETA() == null) {
//            for (Machine m : c.getMachines()) {
////                Machine m = machineRepository.findOne(mID);
//                if (m.getETA() > c.getCurrentETA()) {
//                    c.setCurrentETA(m.getETA());
//                }
//            }
            c.setCurrentETA(time);
        }

        if (c.getId() == 0)
            c.setId(c_id++);

//        LOG.info("Sending coalition " + c.getId() + "with size " + c.getMachines().size());

        checkDuplicates(c);
        coalitionClient.sendCoalition(c);
    }

    public void sendCoalitionToComponent(Coalition c, Long time, int componentIndex){

        if(time / 10000 < 10)
            time = time * Configuration.TIME_DIVISOR;
        if(c.getCurrentETA() == null) {
            c.setCurrentETA(time);
        }

        if (c.getId() == 0)
            c.setId(c_id++);


//        LOG.info("Sending coalition " + c.getId() + "with size " + c.getMachines().size());

        checkDuplicates(c);
        coalitionClient.sendCoalitionToComponent(c, componentIndex);
    }

    private void checkDuplicates(Coalition c) {
        if(c.getMachines().size() > 1) {
            Double mincpu = Double.MAX_VALUE;
            LOG.info("Sending coalition");
            int duplicates = 0;
            for(Machine m : c.getMachines()){
                if(mincpu > m.getCpu())
                    mincpu = m.getCpu();
                boolean self = false;
                for(Machine m1: c.getMachines()){
                    if(m1.getId().equals(m.getId())){
                        if(self){
                            duplicates++;
                        }else{
                            self = true;
                        }
                    }
                }
            }
            LOG.info(String.format("id: %d\nsize: %d\neta: %d\nmin_cpu: %.4f\nduplicates: %d\n",
                    c.getId(), c.getMachines().size(), c.getCurrentETA(), mincpu, duplicates));
        }
    }

    public static void printCoaliion(Coalition c) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(c));
    }

    public void reasonMachineRandomly(Machine machineProperties, Map<Long, Coalition> coalitionMap, long time) throws Exception {


        long minSize;
        Coalition coalition = new Coalition();
        coalition.setCurrentETA(0L);
        coalition.setConfidenceLevel(.0);


//        LOG.info("min size: " + minSize);
//        LOG.info("machine usage size: " + machineProperties.getTaskUsageList().size());

        //machineProperties.setTaskUsageList(null);

        if (coalition.getMachines() == null)
            coalition.setMachines(new ArrayList<Machine>());
        coalition.getMachines().add(machineProperties);


//        LOG.info("Min size for coalition " + minSize);

        minSize = new Random(System.currentTimeMillis()).nextInt(100);
        if (!coalitionMap.containsKey(minSize)) {
            coalitionMap.put(minSize, coalition);
        } else {
            //get last coalition and check if it has enough machines assigned, if yes, then add the created coalition,
            //otherwise add the Machine to the last coalition.
            if (coalitionMap.containsKey(minSize)) {
                Coalition coalition2 = coalitionMap.get(minSize);
                if (coalition2.getMachines().size() == minSize) {
                    coalition2.setId(c_id++);
                    sendCoalition(coalition2, time);
                    coalitionMap.put(minSize, coalition);
                } else {
                    if (coalition2.getJobs() == null)
                        coalition2.setJobs(new TreeMap<String, Long>());
                    if (coalition.getJobs() != null)
                        coalition2.getJobs().putAll(coalition.getJobs());
                    coalition2.getMachines().add(machineProperties);
                }
            }
        }
    }

    public List<Coalition> createCoalitions(List<Machine> machines, Long time, int coalitionStrategyId){
        Map<Long, Long> machineMaxTaskMap = new TreeMap<>();
        List<Job> jobList = jobRepository.findBySubmitTimeBetween((time - Configuration.STEP * App.historySize) *
                Configuration.TIME_DIVISOR - 1, time * Configuration.TIME_DIVISOR +1);
        LOG.info("JobList size: " + jobList.size());
        int maxSize = 0;
        for(Job job : jobList){
            int size = job.getTaskHistory().size();
            if(size > maxSize)
                maxSize = size;
            for(TaskHistory th : job.getTaskHistory().values()){
                if(machineMaxTaskMap.containsKey(th.getMachineId())) {
                    if (machineMaxTaskMap.get(th.getMachineId()) < size)
                        machineMaxTaskMap.put(th.getMachineId(), (long) size);
                }else{
                    machineMaxTaskMap.put(th.getMachineId(), (long) size);
                }

            }
        }
        LOG.info("TaskMaxMapping size: " + machineMaxTaskMap.size());
        List<Coalition> result = null;
        switch (coalitionStrategyId){
            case 0:
                //ACC
                AntColonyClusteringStrategy acs = new AntColonyClusteringStrategy(machineMaxTaskMap);
                result = acs.createCoalitions(machines);
                break;
            case 1:
                //RaL
                RankAndLabelCoalitionStrategy rals = new RankAndLabelCoalitionStrategy(machineMaxTaskMap, maxSize);
                result = rals.createCoalitions(machines);
                break;
            case 2:
                //Random
                break;
        }

        return result;


    }

    @RequestMapping(method = RequestMethod.PUT, path = "/coalitions/update/{time}")
    public void updateAll(@PathVariable Long time) {
//        Map<String, List<Long>> scheduledJobs = new TreeMap<>();
        final Long finalTime = time;
        int i = 0;
        int[] deleted = new int[3];
        int[] created = new int[3];
        int[] total = new int[3];
        for(String type : types){

            Set<Long> scheduledCoalitions =
                    scheduledRepository.findByScheduleType(type).stream().filter(s ->
                            s.getFinishTime() > finalTime)
                            .map(ScheduledJob::getCoalitionId)
                            .collect(Collectors.toSet());
//            LOG.severe(String.format("%s Scheduled Jobs: %d \n", type, scheduledJobs.get(type).size()));

            if(scheduledCoalitions.size() > 0 ){
                LOG.info("Assigned coalitions for type = " + type + ": " + scheduledCoalitions.size());
            }else{
                LOG.severe("No coalition was assigned for type = " + type);
            }
            List<Coalition> toDelete = new ArrayList<>();
            List<Machine> toReorganize = new ArrayList<>();
            final int component = i;

            List<Coalition> coalitionList = coalitionRepository.findByScheduleClass(component);
            if(coalitionList.size() > 0) {
                coalitionList.stream().filter(c -> !scheduledCoalitions.contains(c.getId())).forEach(c -> {
                    coalitionClient.deleteCoalitionFromComponent(c, component);
                    toDelete.add(c);
                    toReorganize.addAll(c.getMachines());
                });
            }

            List<Coalition> reorganized = createCoalitions(toReorganize, time, coalitionStrategyId);

            for (Coalition c : reorganized) {
                sendCoalitionToComponent(c, time, component);
            }
            deleted[component] = toDelete.size();
            created[component] = reorganized.size();
            coalitionRepository.delete(toDelete);
            coalitionRepository.save(reorganized);
            total[component] = coalitionList.size() - toDelete.size() + reorganized.size();
            LOG.info("Coalitions created from reorganization for type = [" + type + "] : " + reorganized.size());

            i++;
        }
        writeResults(time, deleted, created, total);
    }

    private void writeResults(Long time, int[] deleted, int[] created, int[] total) {
        for (int i = 0 ; i < types.length; i++) {
            File f = new File(testOutputPath + "load/" + types[i] + "/coals");
            File dir = new File(testOutputPath + "load/" + types[i]);
            if(!dir.exists())
                dir.mkdirs();
            FileWriter fileWriter = null;
            boolean writeHeader = !f.exists();
            try {
                fileWriter = new FileWriter(f, true);
                if (writeHeader)
                    fileWriter.write("%time #deleted #created #total\n");
                fileWriter.write(String.format("%d %d %d %d\n", time, deleted[i], created[i], total[i]));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (fileWriter != null)
                    try {
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
        }
    }

    private boolean isScheduled(Coalition c) {
        return false;
    }


    public int update(Coalition coalition, long time) {

        for(int i = 0; i < coalition.getMachines().size(); i++){
            Machine mp = machineRepository.findOne(coalition.getMachines().get(i).getId());

            if(mp.getETA() > coalition.getCurrentETA())
                coalition.setCurrentETA(mp.getETA());
            coalition.getMachines().set(i, mp);

            //Check availability of machine
            //The else case was treated in the previous 'for loop' to avoid the same computation
        }

        coalitionRepository.save(coalition);


        //sendCoalition(coalition);
        return 0;
    }
}
