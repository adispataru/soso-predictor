package ro.ieat.soso.predictor.prediction;

import org.springframework.data.annotation.Id;
import ro.ieat.soso.core.prediction.Duration;

/**
 * Created by adrian on 18.01.2016.
 */
public class JobDuration {
    @Id
    private String logicJobName;
    private Long duration;


    public String getLogicJobName() {
        return logicJobName;
    }

    public void setLogicJobName(String logicJobName) {
        this.logicJobName = logicJobName;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

}
