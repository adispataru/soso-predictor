package ro.ieat.soso.predictor.prediction;

import ro.ieat.soso.core.prediction.Prediction;

/**
 * Created by adrian on 07.01.2016.
 */
public class MachinePrediction {
    private Prediction<Double> memoryLoad;
    private Prediction<Double> cpuLoad;


    public Prediction<Double> getMemoryLoad() {
        return memoryLoad;
    }

    public void setMemoryLoad(Prediction<Double> memoryLoad) {
        this.memoryLoad = memoryLoad;
    }

    public Prediction<Double> getCpuLoad() {
        return cpuLoad;
    }

    public void setCpuLoad(Prediction<Double> cpuLoad) {
        this.cpuLoad = cpuLoad;
    }

}
