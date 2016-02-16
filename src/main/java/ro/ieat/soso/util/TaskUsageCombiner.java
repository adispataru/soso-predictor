package ro.ieat.soso.util;

import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;
import ro.ieat.soso.core.jobs.TaskUsage;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by adrian on 25.01.2016.
 * This class contains logic for combining tasks on a given resource.
 */
public class TaskUsageCombiner {
    public static int sampleSize = 300;

    public static TaskUsage combineTaskUsageList(List<TaskUsage> usageList, long startTime,
                                                 List<Job> jobs, List<ScheduledJob> scheduledJobs, String type){

        startTime /= Configuration.TIME_DIVISOR;
        TaskUsage[] total = new TaskUsage[sampleSize];
        for(int i = 0; i < sampleSize; i++){
            total[i] = new TaskUsage();
        }
        for(TaskUsage taskUsage : usageList){
//            long sch = getTimeToStartByJobIdAndScheduleType(taskUsage.getJobId(), type, scheduledJobs);
//            long jobStart = getJobScheduleTime(jobs, taskUsage.getJobId());

            // mindbogglingly offset computation 'cause I can!
//            long offset = (sch != 0 && jobStart != 0) ? (sch - jobStart) / Configuration.TIME_DIVISOR : 0;
//
//            if(Math.abs(offset) > sampleSize) {
//                Logger.getLogger("Combiner").info("job " + taskUsage.getJobId() + " has offset " + offset);
//                Logger.getLogger("Combiner").info("job start" + jobStart + "; scheduled start " + sch);
//                continue;
//            }
            long offset = 0;
            long thisTaskStart = taskUsage.getStartTime() / Configuration.TIME_DIVISOR - startTime;
            long thisTaskEnd = taskUsage.getEndTime() / Configuration.TIME_DIVISOR - startTime;
            for(long i = thisTaskStart + offset; i < thisTaskEnd + offset && i < sampleSize; i++){
                total[(int) i].addTaskUsage(taskUsage);
            }
        }
        TaskUsage result = new TaskUsage();
        for(int i = 0; i < sampleSize; i++){
            result.addTaskUsage(total[i]);
        }
        result.divide(sampleSize);
        return result;

    }

    private static long getTimeToStartByJobIdAndScheduleType(long jobId, String scheduleType, List<ScheduledJob> list){
        for(ScheduledJob scheduledJob : list){
            if(scheduledJob.getJobId() == jobId)
                if (scheduledJob.getScheduleType().equals(scheduleType))
                    return scheduledJob.getTimeToStart();
        }
        return 0;
    }

    private static long getJobScheduleTime(List<Job> list, long jobId){
        for(Job j : list)
            if (j.getJobId() == jobId)
                return j.getScheduleTime();
        return 0;
    }
}
