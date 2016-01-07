package ro.ieat.soso;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ro.ieat.soso.core.config.Configuration;

import java.io.IOException;

/**
 * Created by adrian on 05.01.2016.
 */
@SpringBootApplication
public class App {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration.MACHINE_USAGE_PATH = "./data/output/machine_usage";
        //Predictor.predictMachineUsage(5L, 600, 900);
//        try {
//            Predictor.predictJobRuntime("D7IK6PSGY5Jcf32bkgMfNgBzrXUQs-DhLi4+jCYwZvQ=", 600, Long.MAX_VALUE / Configuration.TIME_DIVISOR);
//        } catch (Exception e) {
//            System.err.printf("Caught error: \n" + e);
//        }

        SpringApplication.run(App.class, args);


    }
}
