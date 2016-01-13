package ro.ieat.soso.reasoning.controllers.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.core.coalitions.Coalition;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public interface CoalitionRepository extends MongoRepository<Coalition, Long>{
    //TODO Check what you put in coalition.jobs
}
