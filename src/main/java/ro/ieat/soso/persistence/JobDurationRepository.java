package ro.ieat.soso.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.predictor.prediction.JobDuration;

import java.util.List;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining job duration predictions...
 */
public interface JobDurationRepository extends MongoRepository<JobDuration, Long>{

    List<JobDuration> findByLogicJobName(String logicJobName);

}
