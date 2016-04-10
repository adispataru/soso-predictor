package ro.ieat.soso.reasoning.startegies;

import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.reasoning.ResourceAbstraction;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by adrian on 2/10/16.
 */
public  abstract class AbstractCoalitionStrategy {


    public abstract List<Coalition> createCoalitions(List<Machine> machines);



    public abstract void learnThreshold(List<? extends ResourceAbstraction> resourceAbstractionList);

}
