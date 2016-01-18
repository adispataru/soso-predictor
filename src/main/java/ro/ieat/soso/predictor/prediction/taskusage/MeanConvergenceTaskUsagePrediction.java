package ro.ieat.soso.predictor.prediction.taskusage;

import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.Predictable;
import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.List;

/**
 * Created by adrian on 18.01.2016.
 * This prediction method computes the average of the usage list.
 */
public class MeanConvergenceTaskUsagePrediction implements PredictionMethod {

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
        long sum11 = 0, sum12 = 0, sum21 = 0, sum22 = 0, sum31 = 0, sum32 = 0;

        for(TaskUsage usage : (List<TaskUsage>) taskUsageList){
            //or max cpu?
            Double max = usage.getMaxCpu();
            Double avg = usage.getCpu();
            sum11 += max;
            sum12 += avg;

            max = usage.getMaxMemory();
            avg = usage.getMemory();
            sum21 += max;
            sum22 += avg;

            max = usage.getMaxDisk();
            avg = usage.getDisk();
            sum31 += max;
            sum32 += avg;
            //set prediction start time to last measurement period
            if(usage.getEndTime() > result.getStartTime())
                result.setStartTime(usage.getEndTime());
        }
        result.setMaxCpu(sum11 * 1.0/taskUsageList.size());
        result.setCpu(sum12 * 1.0 / taskUsageList.size());
        result.setMaxMemory(sum21 * 1.0/taskUsageList.size());
        result.setMemory(sum22 * 1.0/taskUsageList.size());
        result.setMaxDisk(sum31 * 1.0/taskUsageList.size());
        result.setDisk(sum32 * 1.0/taskUsageList.size());
        return result;

    }
}
