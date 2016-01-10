package ro.ieat.soso.predictor.prediction;

import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.core.prediction.MachinePrediction;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Created by adrian on 06.01.2016.
 */
public class PredictionFactory {

    public static Double pickFromHistogram(Map<Double, Long> histogram){
        Random random = new Random();
        Double d = random.nextDouble();
        Long total = 0L;
        if(histogram.size() == 1)
            return histogram.keySet().iterator().next();
        for(Long l : histogram.values()) {
            total += l;
        }
        for(Double key : histogram.keySet()){
            if(d > histogram.get(key)/total)
                return key;
        }
        return .0;

    }

    public static Long pickFromHistogramLong(Map<Long, Long> histogram){
        Random random = new Random();
        Double d = random.nextDouble();
        Long total = 0L;
        if(histogram.size() == 1)
            return histogram.keySet().iterator().next();
        for(Long l : histogram.values())
            total += l;
        for(Long key : histogram.keySet()){
            if(d > histogram.get(key)/total)
                return key;
        }
        return 0L;
    }


    public static MachinePrediction predictMachineUsage(List<Usage> list){
        MachinePrediction result = new MachinePrediction();
        result.setAverageCPU(.0);
        result.setAverageMemory(.0);
        result.setMinCPU(Double.MAX_VALUE);
        result.setMinMemory(Double.MAX_VALUE);
        result.setMaxCPU(.0);
        result.setMaxMemory(.0);
        result.setHistogramCPU(.0);
        result.setHistogramMemory(.0);
        double sum = 0, sum2 = 0;
        Map<Double, Long> histogram = new TreeMap<Double, Long>();
        Map<Double, Long> histogram2 = new TreeMap<Double, Long>();
        for(Usage usage : list){
            //or max cpu?
            Double max = usage.getMaxCpu();
            Double avg = usage.getCpu();
            if(max > result.getMaxCPU()){
                result.setMaxCPU(max);
            }
            if(avg < result.getMinCPU())
                result.setMinCPU(avg);
            sum += avg;
            if (histogram.containsKey(avg))
                histogram.put(avg, histogram.get(avg) + 1);
            else
                histogram.put(avg, 1L);

            max = usage.getMaxMemory();
            avg = usage.getMemory();
            if(max > result.getMaxMemory()){
                result.setMaxMemory(max);
            }
            if(avg < result.getMinMemory())
                result.setMinMemory(avg);
            sum2 += avg;
            if (histogram.containsKey(avg))
                histogram2.put(avg, histogram.get(avg) + 1);
            else
                histogram2.put(avg, 1L);
        }
        if(list.size() > 0){
            result.setAverageCPU(sum/list.size());
            result.setAverageMemory(sum2/list.size());
            result.setHistogramCPU(pickFromHistogram(histogram));
            result.setHistogramMemory(pickFromHistogram(histogram2));
        }else{
            result.setMaxCPU(.0);
            result.setMaxMemory(.0);
            result.setMinCPU(.0);
            result.setMinMemory(.0);
        }
        return result;
    }




    public static DurationPrediction predictTime(List<Long> list){
        DurationPrediction result = new DurationPrediction();
        result.setMax(Long.MIN_VALUE);
        result.setMin(Long.MAX_VALUE);
        result.setAverage(.0);
        result.setHistogram(0L);
        double sum = 0;
        Map<Long, Long> histogram = new TreeMap<Long, Long>();
        for(Long t : list){
            if(t > result.getMax()){
                result.setMax(t);
            }
            if(t < result.getMin())
                result.setMin(t);
            sum += t;
            if (histogram.containsKey(t))
                histogram.put(t, histogram.get(t) + 1);
            else
                histogram.put(t, 1L);
        }
        if(list.size() > 0){
            result.setAverage(sum/list.size());
            result.setHistogram(pickFromHistogramLong(histogram));
        }else{
            return null;
        }
        return result;
    }

    public static DurationPrediction maxLongDurationPrediction(){
        DurationPrediction d = new DurationPrediction();
        d.setHistogram(Long.MAX_VALUE);
        d.setAverage(Double.MAX_VALUE);
        d.setMax(Long.MAX_VALUE);
        d.setMin(Long.MAX_VALUE);
        return d;
    }

}
