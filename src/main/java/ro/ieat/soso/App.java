package ro.ieat.soso;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.config.Configuration;

import java.util.logging.Logger;

/**
 * Created by adrian on 05.01.2016.
 */
@SpringBootApplication
@ComponentScan(basePackages = "ro.ieat.soso")
@EnableMongoRepositories(basePackages = "ro.ieat.soso.persistence")
public class App {
    public static final Long initTime = 2700L;
    public static final Long jobSendingTime = initTime + Configuration.STEP;

    private static Logger LOG = Logger.getLogger(App.class.toString());

    public static void main(String[] args) throws Exception {
        Configuration.MACHINE_USAGE_PATH = "./data/output/machine_usage";
        //Predictor.predictMachineUsage(5L, 600, 900);
//        try {
//            Predictor.predictJobRuntime("D7IK6PSGY5Jcf32bkgMfNgBzrXUQs-DhLi4+jCYwZvQ=", 600, Long.MAX_VALUE / Configuration.TIME_DIVISOR);
//        } catch (Exception e) {
//            System.err.printf("Caught error: \n" + e);
//        }
        //initializeCoalitions();

        //configure(600, 6000);

        SpringApplication.run(App.class, args);
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getForObject("http://localhost:8088/app/init/600/" + Long.MAX_VALUE / Configuration.TIME_DIVISOR, String.class);
        restTemplate.put("http://localhost:8088/app/start/" + initTime + "/9900/4" , null);


    }

}
