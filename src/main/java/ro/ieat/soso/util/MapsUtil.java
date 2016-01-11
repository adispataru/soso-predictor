package ro.ieat.soso.util;

import ro.ieat.soso.core.jobs.Job;

import java.util.*;

/**
 * Created by adrian on 11.01.2016.
 */
public class MapsUtil {

    public static Map<Long, Job> sortJobMaponSubmitTime(final Map<Long, Job> map){
        List<Map.Entry<Long, Job>> entries = new ArrayList<Map.Entry<Long, Job>>(map.size());

        entries.addAll(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<Long, Job>>() {
            public int compare(final Map.Entry<Long, Job> entry1, final Map.Entry<Long, Job> entry2) {
                return new Long(entry1.getValue().getSubmitTime()).compareTo(entry2.getValue().getSubmitTime());
            }
        });

        Map<Long, Job> sortedMap = new LinkedHashMap<Long, Job>();
        for (Map.Entry<Long, Job> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

}
