package ro.ieat.soso.reasoning.controllers.persistence;

import ro.ieat.soso.core.coalitions.Coalition;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by adrian on 10.01.2016.
 * For maintaining coalitions.
 */
public class CoalitionRepository {
    public static Map<Long, Coalition> coalitionMap = new TreeMap<Long, Coalition>();
}
