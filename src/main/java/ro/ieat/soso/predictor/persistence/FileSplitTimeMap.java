package ro.ieat.soso.predictor.persistence;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by adrian on 09.01.2016.
 * This class has the purpose of memoization for the place where events can be found in files based on
 * their timstamp. Current implementation takes 4s to read a job via a REST call. Improvements can be made by
 * assigning each job an offset corresponding to its offset in the file.
 *
 */
public class FileSplitTimeMap {
    private static Map<Long, String> jobEventsMap = new TreeMap<Long, String>();
    private static Map<Long, String> taskEventsMap = new TreeMap<Long, String>();
    private static Map<Long, String> taskUsageMap = new TreeMap<Long, String>();

    public static String getJobEventsFile(long time){
        long prev = 0;
        for(Long key : jobEventsMap.keySet()){
            if(key <= time)
                prev = key;
            if (key > time)
                return jobEventsMap.get(prev);
        }
        return jobEventsMap.get(prev);
    }
    public static String getTaskEventsFile(long time){
        long prev = 0;
        for(Long key : taskEventsMap.keySet()){
            if(key <= time)
                prev = key;
            if (key > time)
                return taskEventsMap.get(prev);
        }
        return taskEventsMap.get(prev);
    }
    public static String getTaskUsageFile(long time){
        long prev = 0;
        for(Long key : taskUsageMap.keySet()){
            if(key <= time)
                prev = key;
            if (key > time)
                return taskUsageMap.get(prev);
        }
        return taskUsageMap.get(prev);
    }

    public static void putJobEventsTime(Long time, String s){
        jobEventsMap.put(time, s);
    }
    public static void putTaskEventsTime(Long time, String s){
        taskEventsMap.put(time, s);
    }
    public static void putTaskUsageTime(Long time, String s){
        taskUsageMap.put(time, s);
    }
}
