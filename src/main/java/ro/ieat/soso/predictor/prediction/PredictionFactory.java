package ro.ieat.soso.predictor.prediction;

import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.prediction.Prediction;

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

    public static Prediction<Double> predictCPU(List<Usage> list){
        Prediction<Double> result = new Prediction<Double>(Double.MIN_VALUE, Double.MAX_VALUE, .0, .0);
        double sum = 0;
        Map<Double, Long> histogram = new TreeMap<Double, Long>();
        for(Usage usage : list){
            //or max cpu?
            Double max = usage.getMaxCpu();
            Double avg = usage.getCpu();
            if(max > result.getMax()){
                result.setMax(max);
            }
            if(avg < result.getMin())
                result.setMin(avg);
            sum += avg;
            if (histogram.containsKey(avg))
                histogram.put(avg, histogram.get(avg) + 1);
            else
                histogram.put(avg, 1L);
        }
        if(list.size() > 0){
            result.setAverage(sum/list.size());
            result.setHistogram(pickFromHistogram(histogram));
        }else{
            result.setMax(.0);
            result.setMin(.0);
            result.setAverage(.0);
            result.setHistogram(.0);
        }
        return result;
    }

    public static Prediction<Double> predictMemory(List<Usage> list){
        Prediction<Double> result = new Prediction<Double>(Double.MIN_VALUE, Double.MAX_VALUE, .0, .0);
        double sum = 0;
        Map<Double, Long> histogram = new TreeMap<Double, Long>();
        for(Usage usage : list){
            //or max memory?
            Double max = usage.getMaxMemory();
            Double avg = usage.getMemory();
            if(max > result.getMax()){
                result.setMax(max);
            }
            if(avg < result.getMin())
                result.setMin(avg);
            sum += avg;
            if (histogram.containsKey(avg))
                histogram.put(avg, histogram.get(avg) + 1);
            else
                histogram.put(avg, 1L);
        }
        if(list.size() > 0){
            result.setAverage(sum/list.size());
            result.setHistogram(pickFromHistogram(histogram));
        }else{
            result.setMax(.0);
            result.setMin(.0);
            result.setAverage(.0);
            result.setHistogram(.0);
        }
        return result;
    }

    public static Prediction<Double> predictDisk(List<Usage> list){
        Prediction<Double> result = new Prediction<Double>(Double.MIN_VALUE, Double.MAX_VALUE, .0, .0);
        double sum = 0;
        Map<Double, Long> histogram = new TreeMap<Double, Long>();
        for(Usage usage : list){
            Double max = usage.getMaxDisk();
            Double avg = usage.getDisk();
            if(max > result.getMax()){
                result.setMax(max);
            }
            if(max < result.getMin())
                result.setMin(max);
            sum += avg;
            if (histogram.containsKey(avg))
                histogram.put(avg, histogram.get(avg) + 1);
        }
        if(list.size() > 0){
            result.setAverage(sum/list.size());
            result.setHistogram(pickFromHistogram(histogram));
        }else{
            result.setAverage(.0);
            result.setHistogram(.0);
        }
        return result;
    }


    public static Prediction<Long> predictTime(List<Long> list){
        Prediction<Long> result = new Prediction<Long>(Long.MIN_VALUE, Long.MAX_VALUE, .0, 0L);
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

    public static Prediction<Double> predictDouble(List<Double> list){
        Prediction<Double> result = new Prediction<Double>(Double.MIN_VALUE, Double.MAX_VALUE, .0, .0);
        double sum = 0;
        Map<Double, Long> histogram = new TreeMap<Double, Long>();
        for(Double t : list){
            if(t > result.getMax()){
                result.setMax(t);
            }
            if(t < result.getMin())
                result.setMin(t);
            sum += t;
            if (histogram.containsKey(t))
                histogram.put(t, histogram.get(t) + 1);
        }
        if(list.size() > 0){
            result.setAverage(sum/list.size());
            result.setHistogram(pickFromHistogram(histogram));
        }else{
            result.setAverage(.0);
            result.setHistogram(.0);
        }
        return result;
    }
}
