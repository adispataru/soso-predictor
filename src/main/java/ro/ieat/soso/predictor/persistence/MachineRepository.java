package ro.ieat.soso.predictor.persistence;

import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.jobs.Job;

import java.util.*;

/**
 * Created by adrian on 08.01.2016.
 */
public class MachineRepository{
    private Map<Long, Machine> machineRepository = new TreeMap<Long, Machine>();
    public Map<Long, Job> jobRepo;
    public List<Long> assignedJobs;

    private static MachineRepository repo;

    private MachineRepository(){
        jobRepo = new TreeMap<Long, Job>();
        assignedJobs = new ArrayList<Long>();
    }

    public static MachineRepository getInstance(){
        if (repo == null)
            repo = new MachineRepository();
        return repo;
    }

    public Machine findOne(Long machineId){
        return machineRepository.get(machineId);
    }

    public Collection<Machine> findAll(){
        return machineRepository.values();
    }

    public void save(Machine m){
        machineRepository.put(m.getId(), m);
    }

}
