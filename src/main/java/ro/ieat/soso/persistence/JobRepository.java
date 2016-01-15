package ro.ieat.soso.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.core.jobs.Job;

import java.util.List;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public interface JobRepository extends MongoRepository<Job, Long>{
    //TODO Check what you put in coalition.jobs
    List<Job> findByLogicJobName(String logicJobName);
    List<Job> findBySubmitTimeBetween(Long min, Long max);
    List<Job> findByLogicJobNameAndSubmitTimeLessThan(String logicJobName, Long max);
}
