import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ro.ieat.soso.App;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.mappers.JobEventsMapper;
import ro.ieat.soso.core.mappers.TaskEventsMapper;
import ro.ieat.soso.core.prediction.Prediction;
import ro.ieat.soso.predictor.Predictor;
import ro.ieat.soso.reasoning.CoalitionReasoner;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Created by adrian on 07.01.2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(App.class)
public class CoalitionReasonerTest {

    @Test
    public void initializeCoalitionsTest() throws Exception {
        String machineUsagePath = "./data/output/machine_usage/";

        int i = 0;
        int numMachines = 10;
        for(File f : new File(machineUsagePath).listFiles()){

            Predictor.predictMachineUsage(Long.parseLong(f.getName()), 600, 5400);


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

        CoalitionReasoner.currentJobs = jobMap;
        CoalitionReasoner.appDurationMap = new TreeMap<String, Prediction<Long>>();
        for(Job j : jobMap.values()){
//            if(CoalitionReasoner.appDurationMap.containsKey(j.getLogicJobName()))
//                continue;
            Predictor.predictJobRuntime(j.getLogicJobName(), 600, 5400);
        }

        CoalitionReasoner.initCoalitions(5400);
        List<Coalition> cs = CoalitionReasoner.coalitionCollector;
        assertTrue("Coalition size", cs.size() > 0);
        long sum = 0;
        for(Coalition c : cs){
            sum += c.getMachines().size();
        }
        assertEquals(numMachines, sum);



    }
}
