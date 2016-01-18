package ro.ieat.soso.predictor.prediction;

import org.springframework.data.annotation.Id;
import ro.ieat.soso.core.prediction.Duration;

/**
 * Created by adrian on 18.01.2016.
 */
public class JobDuration {
    @Id
    private Long id;
    private String logicJobName;
    private Long submitTime;
    private Duration duration;


    public String getLogicJobName() {
        return logicJobName;
    }

    public void setLogicJobName(String logicJobName) {
        this.logicJobName = logicJobName;
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    public Long getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Long submitTime) {
        this.submitTime = submitTime;
    }
}
