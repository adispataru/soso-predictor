package ro.ieat.soso.predictor.prediction.taskusage;

import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.Predictable;
import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.List;

/**
 * Created by adrian on 18.01.2016.
 * This prediction method uses LinearRegresion to find a solution to the graph of the task usage.
 */
public class LRTaskUsagePrediction implements PredictionMethod {

    @Override
    public Predictable predict(List<? extends Predictable> list) {
        if(list.get(0) instanceof TaskUsage)
            return predictTaskUsage(list);
        return null;
    }

    public TaskUsage predictTaskUsage(List<? extends Predictable> taskUsageList){

        return null;
    }
}
