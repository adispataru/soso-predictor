package ro.ieat.soso.predictor.persistence;

import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by adrian on 08.01.2016.
 */
public class MachineRepository{
    private static Map<Long, Machine> machineRepository = new TreeMap<Long, Machine>();
    public static Map<Long, Job> jobRepo;
    public static List<Long> assignedJobs;

    public static Machine findOne(Long machineId){
        return machineRepository.get(machineId);
    }

    public static void save(Machine m){
        machineRepository.put(m.getId(), m);
    }

}
