package ro.ieat.soso.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.core.jobs.TaskUsage;

import java.util.List;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public interface TaskUsageMappingRepository extends MongoRepository<TaskUsage, Long>{
    //TODO Check what you put in coalition.jobs
    TaskUsage findByJobIdAndTaskIndex(long jobId, long taskIndex);
    List<TaskUsage> findByMachineId(Long machineId);
    List<TaskUsage> findByMachineIdAndStartTimeLessThan(Long machineId, Long startTime);
    List<TaskUsage> findByStartTimeGreaterThanAndFinishTimeLessThan(Long startTime, Long endTime);
    List<TaskUsage> findByJobId(Long jobId);
}
