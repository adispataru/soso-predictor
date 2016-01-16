package ro.ieat.soso.evaluator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.persistence.CoalitionRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.persistence.TestTaskRepository;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by adrian on 13.01.2016.
 */
@RestController
public class Evaluator {
    @Autowired
    CoalitionRepository coalitionRepository;
    @Autowired
    MachineRepository machineRepository;
    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    TestTaskRepository testTaskRepository;
    RestTemplate template = new RestTemplate();
    private static final String testOutputPath = "./output/results/";

    public static boolean containsTaskUsageId(Collection<TestTaskUsage> c, long location) {
        for(TestTaskUsage o : c) {
            if(o != null && o.taskUsageId == location) {
                return true;
            }
        }
        return false;
    }

    private static void writeResults(long time, List<Double> cpuLoad, List<Double> memLoad, List<Long> lateList, long numViolations){

        double avgCPU = 0, avgMem = 0, avgLate = 0;
        for(Double d : cpuLoad)
            avgCPU+= d;
        for(Double d : memLoad)
            avgMem+= d;
        for(Long l : lateList)
            avgLate += l;


        File f = new File(testOutputPath + "initial");
        boolean writeHeader = f.exists();
        try {
            FileWriter fileWriter = new FileWriter(f, true);
            if(writeHeader)
                fileWriter.write("%time avgCPU avgMEM avgLATE numViol");
            fileWriter.write(String.format("%d %f %f %f %d", time, avgCPU/cpuLoad.size(), avgMem/memLoad.size(),
                    avgLate/lateList.size(), numViolations));

        } catch (IOException e) {
            Logger.getLogger("Evaluator").severe("File not found " + f.getAbsolutePath());
        }
    }

    @RequestMapping(method = RequestMethod.PUT, path = "/evaluate/{time}")
    public void evaluate(@PathVariable Long time){

        List<TestTaskUsage> allTestTaskUsageList = testTaskRepository.findAll();
        long lowTime = (time - Configuration.STEP) * Configuration.TIME_DIVISOR;
        long wrongTasks = 0;
        Logger LOG = Logger.getLogger("Evaluator");

        List<Double> cpuLoadList = new ArrayList<>();
        List<Double> memLoadList = new ArrayList<>();
        List<Long> latenessList = new ArrayList<>();
        long numViolations = 0;
        for(Coalition c : coalitionRepository.findAll()){
            List<TestTaskUsage> testTaskUsageList = allTestTaskUsageList.stream().filter(
                    t -> t.coalitionId == c.getId()
            ).collect(Collectors.toList());

            // here predict coalition confidence level and jobs

            for(TestTaskUsage testTaskUsage : testTaskUsageList){
                String jobName = testTaskUsage.logicJobname;
                LOG.info("Predict job at time " + time);
                String predictionPath = "http://localhost:8088/predict/job/" + jobName + "/" + time;
                DurationPrediction dp = template.getForObject(predictionPath, DurationPrediction.class);
                if(c.getJobs() == null)
                    c.setJobs(new TreeMap<>());
                c.getJobs().put(jobName, dp.getMax());
                //Machine m = machineRepository.findOne(testTaskUsage.machineId);
                List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                        findByMachineIdAndStartTimeLessThan(testTaskUsage.machineId, time);

                Double cpu = .0;
                Double mem = .0;
                for(TaskUsage taskUsage : taskUsageList){
                    //TODO Add all usages except the ones in allTestTaskUsageList
                    if(!containsTaskUsageId(allTestTaskUsageList, taskUsage.getId())) {
                        continue;
                    }
                    if(taskUsage.getStartTime() < testTaskUsage.startTime){
                        numViolations++;
                        latenessList.add(taskUsage.getStartTime() - testTaskUsage.startTime);
                    }
                    for(Usage usage : taskUsage.getUsageList()){
                        if(usage.getStartTime() < lowTime)
                            continue;
                        if(usage.getEndTime() > time * Configuration.TIME_DIVISOR)
                            break;
                        cpu += usage.getMaxCpu();
                        mem += usage.getMaxMemory();

                    }

                }
                TaskUsage taskUsage = taskUsageMappingRepository.findOne(testTaskUsage.taskUsageId);
                for(Usage usage : taskUsage.getUsageList()){
                    if(usage.getStartTime() < lowTime)
                        continue;
                    if(usage.getEndTime() > time * Configuration.TIME_DIVISOR)
                        break;
                    cpu += usage.getMaxCpu();
                    mem += usage.getMaxMemory();

                }

                //Reason CPU
                Machine m  = machineRepository.findOne(testTaskUsage.machineId);
                if(m.getCpu() - cpu < 0.2*m.getCpu() && m.getMemory() - mem < 0.2*m.getMemory()){
                    wrongTasks++;
                    numViolations++;
                }

                //LOG machine load, coalition load, other things.
                //TODO LOG machine load
                cpuLoadList.add(cpu/m.getCpu());
                memLoadList.add(mem/m.getMemory());

            }
            //TODO LOG machine idle time
            if(testTaskUsageList.size() == 0)

            //TODO LOG lateness
            //TODO LOG no of violations
            c.setConfidenceLevel(c.getConfidenceLevel() * wrongTasks/c.getMachines().size());
        }

        writeResults(time, cpuLoadList, memLoadList, latenessList, numViolations);


    }
}
