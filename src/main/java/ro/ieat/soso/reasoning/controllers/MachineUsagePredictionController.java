package ro.ieat.soso.reasoning.controllers;

import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.reasoning.CoalitionReasoner;

import java.util.TreeMap;

/**
 * Created by adrian on 07.01.2016.
 */

public class MachineUsagePredictionController {


    //Eventually this should be a REST method.
    public static void updateMachineStatus(long id, Machine prediction){
            if(CoalitionReasoner.machinePredictionMap == null){
                CoalitionReasoner.machinePredictionMap = new TreeMap<Long, Machine>();
            }
            CoalitionReasoner.machinePredictionMap.put(id, prediction);
    }


}
