package ro.ieat.soso.predictor.prediction.taskusage;

import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.Predictable;
import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.List;

/**
 * Created by adrian on 18.01.2016.
 * This prediction method optimistically considers the maximum historical task usage as best the candidate.
 */
public class PessimisticTaskUsagePrediction implements PredictionMethod {

    @Override
    public Predictable predict(List<? extends Predictable> list) {
        if(list.get(0) instanceof TaskUsage)
            return predictTaskUsage(list);
        return null;
    }

    public TaskUsage predictTaskUsage(List<? extends Predictable> taskUsageList){
        TaskUsage result = new TaskUsage();
        result.setCpu(.0);
        result.setMemory(.0);
        result.setMaxCpu(.0);
        result.setMaxMemory(.0);
        result.setDisk(.0);
        result.setMaxDisk(.0);
        result.setStartTime(0L);

        for(TaskUsage usage : (List<TaskUsage>) taskUsageList){
            //or max cpu?
            Double max = usage.getMaxCpu();
            Double avg = usage.getCpu();
            if(max > result.getMaxCpu())
                result.setMaxCpu(max);
            if(avg > result.getCpu())
                result.setCpu(avg);

            max = usage.getMaxMemory();
            avg = usage.getMemory();
            if(max > result.getMaxMemory())
                result.setMaxMemory(max);
            if(avg > result.getMemory())
                result.setMemory(avg);

            max = usage.getMaxDisk();
            avg = usage.getDisk();
            if(max > result.getMaxDisk())
                result.setMaxDisk(max);
            if(avg > result.getDisk())
                result.setDisk(avg);


            //set prediction start time to last measurement period
            if(usage.getEndTime() > result.getStartTime())
                result.setStartTime(usage.getEndTime());

        }
        return result;

    }
}
