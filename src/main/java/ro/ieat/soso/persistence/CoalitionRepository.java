package ro.ieat.soso.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;

import java.util.List;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public interface CoalitionRepository extends MongoRepository<Coalition, Long>{
    //TODO Check what you put in coalition.jobs
    List<Coalition> findByLogicJobName(String logicJobName);
    Coalition findByMachinesId(Long id);
    List<Coalition> findByScheduleClass(long scheduleClass);
}
