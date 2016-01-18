import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ro.ieat.soso.App;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.mappers.JobEventsMapper;
import ro.ieat.soso.core.mappers.TaskEventsMapper;
import ro.ieat.soso.persistence.CoalitionRepository;
import ro.ieat.soso.reasoning.CoalitionReasoner;
import util.AppConfig;
import util.TestConfig;

import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.Assert.assertTrue;

/**
 * Created by adrian on 07.01.2016.
 * Testing initialization and reason for coalitioReasoner.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(App.class)
@ContextConfiguration(classes = {AppConfig.class, TestConfig.class})
@Profile("test")
public class CoalitionReasonerTest {

    @Autowired
    CoalitionRepository coalitionRepository;

    @Test
    public void initializeCoalitionsTest() throws Exception {
        String machineUsagePath = "./data/output/machine_usage/";

        int i = 0;
        int numMachines = 10;
        File[] machineFiles = new File(machineUsagePath).listFiles();
        for(File f : machineFiles){

            //assertEquals(0, Predictor.predictMachineUsage(Long.parseLong(f.getName()), 600, 5400));


            if(++i >= numMachines)
                break;
        }

        Map<Long, Job> jobMap = new TreeMap<Long, Job>();
        for(File f : new File(Configuration.JOB_EVENTS_PATH).listFiles()) {
            JobEventsMapper.map(new FileReader(f), jobMap, 600, 5400);
        }
        for(File f : new File(Configuration.TASK_EVENTS_PATH).listFiles()) {
            TaskEventsMapper.map(new FileReader(f), jobMap, 600, 5400);
        }

        for(Job j : jobMap.values()){
//            if(CoalitionReasoner.appDurationMap.containsKey(j.getLogicJobName()))
//                continue;
            //assertEquals(0, Predictor.predictJobRuntime(j.getLogicJobName(), 600, 5400));
        }

        CoalitionReasoner cr = new CoalitionReasoner();
        cr.initCoalitions(5400);
        Collection<Coalition> cs = coalitionRepository.findAll();
        assertTrue("Coalition size", cs.size() > 0);
        long sum = 0;
        for(Coalition c : cs){
            sum += c.getMachines().size();
        }
        //assertEquals(numMachines, sum);

        //Test coalition reason

        i = 0;
        for(File f : machineFiles){

            //assertEquals(0, Predictor.predictMachineUsage(Long.parseLong(f.getName()), 900, 5700));


            if(++i >= numMachines)
                break;
        }
        for(File f : new File(Configuration.JOB_EVENTS_PATH).listFiles()) {
            JobEventsMapper.map(new FileReader(f), jobMap, 5400, 5700);
        }
        for(File f : new File(Configuration.TASK_EVENTS_PATH).listFiles()) {
            TaskEventsMapper.map(new FileReader(f), jobMap, 5400, 5700);
        }

        for(Job j : jobMap.values()){
//            if(CoalitionReasoner.appDurationMap.containsKey(j.getLogicJobName()))
//                continue;
           //assertEquals(0, Predictor.predictJobRuntime(j.getLogicJobName(), 5400, 5700));
        }


//        for(Coalition c : coalitionRepository.findAll()){
//            long endTime = c.getMachines().get(0).getPrediction().getEndTime();
//            assertEquals(0, CoalitionReasoner.reason(c, 5700));
//            long newEndTime = c.getMachines().get(0).getPrediction().getEndTime();
//            assertEquals(endTime + Configuration.STEP * Configuration.TIME_DIVISOR, newEndTime);
//        }




    }
}
