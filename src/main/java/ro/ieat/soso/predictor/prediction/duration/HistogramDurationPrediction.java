package ro.ieat.soso.predictor.prediction.duration;

import ro.ieat.soso.core.prediction.Duration;
import ro.ieat.soso.core.prediction.Predictable;
import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by adrian on 18.01.2016.
 * This prediction method selects a solution based on histogram of data.
 */
public class HistogramDurationPrediction implements PredictionMethod {

    @Override
    public Predictable predict(List<? extends Predictable> list) {
        if(list.get(0) instanceof Duration)
            return predictTaskUsage(list);
        return null;
    }

    public Duration predictTaskUsage(List<? extends Predictable> taskUsageList){
        Duration result = null;

        Map<Long, Long> histogram = new TreeMap<>();

        for(Duration usage : (List<Duration>) taskUsageList){
            if(histogram.containsKey(usage))
                histogram.put(usage.longValue(), histogram.get(usage.longValue()) + 1);
            else{
                histogram.put(usage.longValue(), 1L);
            }

            //set prediction start time to last measurement period
        }

        Random random = new Random();
        Double d = random.nextDouble();
        Long total = 0L;
        if(histogram.size() == 1)
            result =  new Duration(histogram.keySet().iterator().next());

        for(Long key : histogram.keySet()){
            if(d > histogram.get(key)/total) {
                result = new Duration(key);
                break;
            }
        }


        return result;
    }
}
