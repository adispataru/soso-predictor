import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ro.ieat.soso.App;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.persistence.CoalitionRepository;
import util.TestConfig;

import java.util.ArrayList;

/**
 * Created by adrian on 12.01.2016.
 * To test mongo db
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(App.class)
@ContextConfiguration(classes = {App.class, TestConfig.class})
@Profile("test")
public class MongoTest {
    @Autowired
    private CoalitionRepository coalitionRepository;

    @Test
    public void test(){
        Coalition c = new Coalition();
        c.setLogicJobName("ala bala");
        c.setMachines(new ArrayList<Long>());
        Machine m = new Machine(1, 0.5, 0.5);

        c.getMachines().add(m.getId());
        coalitionRepository.save(c);
        Coalition c2 = coalitionRepository.findAll().get(0);
        Assert.assertEquals(c2.getLogicJobName(), c.getLogicJobName());

    }


}
