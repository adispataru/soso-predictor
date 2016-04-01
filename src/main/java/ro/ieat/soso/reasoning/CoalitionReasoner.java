package ro.ieat.soso.reasoning;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.xml.DocumentDefaultsDefinition;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import ro.ieat.soso.App;
import ro.ieat.soso.JobRequester;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.evaluator.FineTuner;
import ro.ieat.soso.persistence.*;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;
import ro.ieat.soso.reasoning.startegies.AntColonyClusteringStrategy;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
        long size = machineRepository.count();

        List<Coalition> coalitions = antColonyClustering(machineRepository.findAll(), time);

        for (Coalition c : coalitions) {
            sendCoalition(c, time);
        }
        coalitionRepository.save(coalitions);
        LOG.info("Coalitions created: " + coalitionRepository.count());
        return coalitions.size();
    }


    public void sendCoalition(Coalition c, long time) {

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

    public List<Coalition> antColonyClustering(List<Machine> machines, Long time){
        Map<Long, Long> machineMaxTaskMap = new TreeMap<>();
        List<Job> jobList = jobRepository.findBySubmitTimeBetween((time - Configuration.STEP * App.historySize) *
                Configuration.TIME_DIVISOR - 1, time * Configuration.TIME_DIVISOR +1);
        LOG.info("JobList size: " + jobList.size());
        for(Job job : jobList){
            int size = job.getTaskHistory().size();
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
        AntColonyClusteringStrategy acs = new AntColonyClusteringStrategy(machineMaxTaskMap);
        List<Coalition> result = acs.clusterize(machines);
        return result;


    }

    @RequestMapping(method = RequestMethod.PUT, path = "/coalitions/update/{time}")
    public void updateAll(@PathVariable Long time) {
//        Map<String, List<Long>> scheduledJobs = new TreeMap<>();
        String[] types = JobRequester.types;
        final Long finalTime = time;
        int i = 0;
        for(String type : types){
           List<Long> scheduledCoalitions =
                    scheduledRepository.findByScheduleType(type).stream().filter(s ->
                            s.getTimeToStart() > (finalTime - Configuration.STEP *Configuration.TIME_DIVISOR))
                            .map(ScheduledJob::getCoalitionId)
                            .collect(Collectors.toList());
//            LOG.severe(String.format("%s Scheduled Jobs: %d \n", type, scheduledJobs.get(type).size()));
            List<Coalition> toDelete = new ArrayList<>();
            List<Machine> toReorganize = new ArrayList<>();
            final int component = i;

            coalitionRepository.findAll().stream().filter(c -> !scheduledCoalitions.contains(c.getId())).forEach(c -> {
                coalitionClient.deleteCoalitionFromComponent(c, component);
                toDelete.add(c);
                toReorganize.addAll(c.getMachines());
            });

            coalitionRepository.delete(toDelete);

            List<Coalition> reorganized = antColonyClustering(toReorganize, time);

            for (Coalition c : reorganized) {
                sendCoalitionToComponent(c, time, component);
            }
            coalitionRepository.save(reorganized);
            LOG.info("Coalitions created from reorganization for type = [" + type + "] : " + coalitionRepository.count());

            i++;
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
