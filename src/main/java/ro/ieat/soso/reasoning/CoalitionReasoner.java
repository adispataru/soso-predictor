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
import ro.ieat.soso.core.mappers.MachineEventsMapper;
import ro.ieat.soso.persistence.CoalitionRepository;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.reasoning.controllers.CoalitionClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Logger;

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


    @RequestMapping(method = RequestMethod.GET, path = "/coalitions/init/{time}")
    public Integer initCoalitions(@PathVariable long time) throws Exception {


        Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();

        int i = 1;
        long size = machineRepository.count();

        for (Machine m : machineRepository.findAll()) {
            reasonMachineRandomly(m, coalitionMap, time);
//            LOG.info("Machine number: " + i);
            if (i % size / 10 == 0)
                LOG.info(String.format("%.2f%%", i * 1.0 / size));
            i++;
        }

        LOG.info("Coalitions created: " + coalitionMap.size());
        for (Coalition c : coalitionMap.values()) {
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

        if (c.getId() == 0)
            c.setId(c_id++);

//        LOG.info("Sending coalition " + c.getId() + "with size " + c.getMachines().size());

        coalitionClient.sendCoalition(c);
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

        if (MachineEventsMapper.MACHINES.containsKey(machineProperties.getId())) {
            machineProperties.setCpu(MachineEventsMapper.MACHINES.get(machineProperties.getId()).getKey());
            machineProperties.setMemory(MachineEventsMapper.MACHINES.get(machineProperties.getId()).getValue());
        }

//        LOG.info("min size: " + minSize);
//        LOG.info("machine usage size: " + machineProperties.getTaskUsageList().size());

        //machineProperties.setTaskUsageList(null);

        if (coalition.getMachines() == null)
            coalition.setMachines(new ArrayList<Long>());
        coalition.getMachines().add(machineProperties.getId());


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
                    sendCoalition(coalition2);
                    coalitionMap.put(minSize, coalition);
                } else {
                    if (coalition2.getJobs() == null)
                        coalition2.setJobs(new TreeMap<String, Long>());
                    if (coalition.getJobs() != null)
                        coalition2.getJobs().putAll(coalition.getJobs());
                    coalition2.getMachines().add(machineProperties.getId());
                }
            }
        }
    }

    @RequestMapping(method = RequestMethod.PUT, path = "/coalitions/update/{time}")
    public void updateAll(@PathVariable long time) {
        for (Coalition c : coalitionRepository.findAll()) {
            update(c, time);
            c = coalitionRepository.findOne(c.getId());
            if (c.getCurrentETA() > (time + Configuration.STEP) * Configuration.TIME_DIVISOR)
                sendCoalition(c);
        }
    }



    public int update(Coalition coalition, long time) {

        for(Long m : coalition.getMachines()){
            Machine mp = machineRepository.findOne(m);

            if(mp.getETA() > coalition.getCurrentETA())
                coalition.setCurrentETA(mp.getETA());

            //Check availability of machine
            //The else case was treated in the previous 'for loop' to avoid the same computation
        }

        coalitionRepository.save(coalition);


        //sendCoalition(coalition);
        return 0;
    }
}
