package ro.ieat.soso.predictor.prediction.duration;

import ro.ieat.soso.core.prediction.Duration;
import ro.ieat.soso.core.prediction.Predictable;
import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.List;

/**
 * Created by adrian on 18.01.2016.
 * This prediction method computes the average of the duration list.
 */
public class MeanConvergenceDurationPrediction implements PredictionMethod {

    @Override
    public Predictable predict(List<? extends Predictable> list) {
        if(list.get(0) instanceof Duration)
            return predictTaskUsage(list);
        return null;
    }

    public Duration predictTaskUsage(List<? extends Predictable> taskUsageList){

        Long sum = 0L;

        for(Duration usage : (List<Duration>) taskUsageList){
            sum += usage.longValue();
            //set prediction start time to last measurement period
        }
        return new Duration(sum / taskUsageList.size());
    }
}
