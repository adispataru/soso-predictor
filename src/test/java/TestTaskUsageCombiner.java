import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ro.ieat.soso.App;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.util.TaskUsageCombiner;
import util.AppConfig;
import util.TestConfig;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertTrue;

/**
 * Created by adrian on 25.01.2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(App.class)
@ContextConfiguration(classes = {AppConfig.class, TestConfig.class})
@Profile("test")
public class TestTaskUsageCombiner {

    @Test
    public void testTaskUsageCombinerDisjoint(){
        TaskUsage t1 = new TaskUsage(300000000L, 499000000L, 0.3, 0.3, 0.3);
        TaskUsage t2 = new TaskUsage(500000000L, 600000000L, 0.3, 0.3, 0.3);
        List<TaskUsage> taskUsageList = new ArrayList<>();
        taskUsageList.add(t1);
        taskUsageList.add(t2);
//        TaskUsage result = TaskUsageCombiner.combineTaskUsageList(taskUsageList, 300);
//        double epsilon = result.getCpu() - 0.3;
//        assertTrue(0.00001 > epsilon);

    }

}
