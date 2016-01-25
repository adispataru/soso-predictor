package ro.ieat.soso.util;

import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.TaskUsage;

import java.util.List;

/**
 * Created by adrian on 25.01.2016.
 */
public class TaskUsageCombiner {

    public static TaskUsage combineTaskUsageList(List<TaskUsage> usageList, long startTime){

        startTime /= Configuration.TIME_DIVISOR;
        TaskUsage[] total = new TaskUsage[300];
        for(int i = 0; i < 300; i++){
            total[i] = new TaskUsage();
        }
        for(TaskUsage taskUsage : usageList){
            long thisTaskStart = taskUsage.getStartTime() / Configuration.TIME_DIVISOR - startTime;
            long thisTaskEnd = taskUsage.getEndTime() / Configuration.TIME_DIVISOR - startTime;
            for(long i = thisTaskStart; i < thisTaskEnd; i++){
                total[(int) i].addTaskUsage(taskUsage);
            }
        }
        TaskUsage result = new TaskUsage();
        for(int i = 0; i < 300; i++){
            result.addTaskUsage(total[i]);
        }
        result.divide(300);
        return result;

    }
}
