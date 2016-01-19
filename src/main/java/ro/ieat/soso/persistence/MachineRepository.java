package ro.ieat.soso.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import ro.ieat.soso.core.coalitions.Machine;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public interface MachineRepository extends MongoRepository<Machine, Long>{
}
