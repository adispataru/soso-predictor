package util;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import ro.ieat.soso.core.config.Configuration;

import java.util.logging.Logger;

/**
 * Created by adrian on 18.01.2016.
 */
@SpringBootApplication
@ComponentScan(basePackages = "ro.ieat.soso")
@EnableMongoRepositories(basePackages = "ro.ieat.soso.persistence")

public class AppConfig {


        private static Logger LOG = Logger.getLogger(AppConfig.class.toString());

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

            SpringApplication.run(AppConfig.class, args);
        }
}
