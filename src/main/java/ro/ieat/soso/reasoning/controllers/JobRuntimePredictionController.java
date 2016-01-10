package ro.ieat.soso.reasoning.controllers;

import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.reasoning.CoalitionReasoner;

import java.util.TreeMap;

/**
 * Created by adrian on 07.01.2016.
 */

public class JobRuntimePredictionController {


    //Eventually this should be a REST method.
    public static void updateJobDuration(String id, DurationPrediction prediction){
        if(CoalitionReasoner.appDurationMap == null)
            CoalitionReasoner.appDurationMap = new TreeMap<String, DurationPrediction>();
            CoalitionReasoner.appDurationMap.put(id, prediction);
    }


}
