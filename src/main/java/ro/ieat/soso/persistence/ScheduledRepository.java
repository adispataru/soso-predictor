package ro.ieat.soso.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.ScheduledJob;

import java.util.List;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public interface ScheduledRepository extends MongoRepository<ScheduledJob, Long>{
    List<ScheduledJob> findBySubmitTimeBetween(Long min, Long max);
    ScheduledJob findByJobIdAndScheduleType(Long jobId, String scheduleType);
    List<ScheduledJob> findByScheduleType(String scheduleType);
}
