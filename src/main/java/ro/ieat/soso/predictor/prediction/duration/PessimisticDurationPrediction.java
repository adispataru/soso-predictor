package ro.ieat.soso.predictor.prediction.duration;

import ro.ieat.soso.core.prediction.Duration;
import ro.ieat.soso.core.prediction.Predictable;
import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.List;

/**
 * Created by adrian on 18.01.2016.
 * This prediction method optimistically considers the minimum historical task usage as best candidate.
 */
public class PessimisticDurationPrediction implements PredictionMethod {

    @Override
    public Predictable predict(List<? extends Predictable> list) {

        if(list.get(0) instanceof Duration)
            return predictTaskDuration(list);
        return null;
    }

    public Duration predictTaskDuration(List<? extends Predictable> taskUsageList){
        Duration result = new Duration(Long.MIN_VALUE);


        for(Duration usage : (List<Duration>) taskUsageList){
            if(usage.longValue() > result.longValue())
                result = new Duration (usage.longValue());
            //set prediction start time to last measurement period
        }
        return result;

    }
}
