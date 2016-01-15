package util;

import com.github.fakemongo.Fongo;
import com.mongodb.Mongo;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * Created by adrian on 14.01.2016.
 */
@Configuration
@EnableMongoRepositories({"ro.ieat.soso.reasoning.controllers.persistence"})
public class TestConfig extends AbstractMongoConfiguration {
//    @Autowired
//    private Environment env;

    @Override
    protected String getDatabaseName() {
        //return env.getRequiredProperty("mongo.db.name");
        return "e-store";
    }

    @Override
    public Mongo mongo() throws Exception {
        return new Fongo(getDatabaseName()).getMongo();
    }
}