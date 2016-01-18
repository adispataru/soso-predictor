package ro.ieat.soso.predictor.prediction;

import ro.ieat.soso.core.prediction.PredictionMethod;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by adrian on 06.01.2016.
 * Prediction factory is used to keep persistence of predicting methods.
 */
public class PredictionFactory {

    public static Map<String, PredictionMethod> predictionMethodMap = new HashMap<>();

    public static PredictionMethod getPredictionMethod(String predictable){
        return predictionMethodMap.get(predictable);
    }

    public static void setPredictionMethod(String predictable, PredictionMethod predictionMethod){
        predictionMethodMap.put(predictable, predictionMethod);
    }

}
