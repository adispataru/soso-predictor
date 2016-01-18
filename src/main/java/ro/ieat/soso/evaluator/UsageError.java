package ro.ieat.soso.evaluator;

import ro.ieat.soso.core.jobs.TaskUsage;

/**
 * Created by adrian on 18.01.2016.
 * This class is used by FineTuner to compute average usage prediction error
 */
public class UsageError {

    private double cpu;
    private double memory;
    private double disk;
    private double maxCpu;
    private double maxMemory;
    private double maxDisk;

    public UsageError(TaskUsage actual, TaskUsage predicted){
        this.cpu = predicted.getCpu() - actual.getCpu();
        this.memory = predicted.getMemory() - actual.getMemory();
        this.disk = predicted.getDisk() - actual.getDisk();
        this.maxCpu = predicted.getMaxCpu() - actual.getMaxCpu();
        this.maxMemory = predicted.getMaxMemory() - actual.getMaxMemory();
        this.maxDisk = predicted.getMaxDisk() - actual.getMaxDisk();
    }

    private UsageError(){
        this.cpu = .0;
        this.memory = .0;
        this.disk = .0;
        this.maxCpu = .0;
        this.maxMemory = .0;
        this.maxDisk = .0;
    }

    public static UsageError getZeroUsageError(){
        return new UsageError();
    }

    public void addError(UsageError error){
        this.cpu += error.getCpu();
        this.memory += error.getMemory();
        this.disk += error.getDisk();
        this.maxCpu += error.getMaxCpu();
        this.maxMemory += error.getMaxMemory();
        this.maxDisk += error.getMaxDisk();
    }

    public void divide(int l){
        this.cpu /= l;
        this.memory /= l;
        this.disk /= l;
        this.maxCpu /= l;
        this.maxMemory /= l;
        this.maxDisk /= l;
    }

    public double getCpu() {
        return cpu;
    }

    public void setCpu(double cpu) {
        this.cpu = cpu;
    }

    public double getMemory() {
        return memory;
    }

    public void setMemory(double memory) {
        this.memory = memory;
    }

    public double getDisk() {
        return disk;
    }

    public void setDisk(double disk) {
        this.disk = disk;
    }

    public double getMaxCpu() {
        return maxCpu;
    }

    public void setMaxCpu(double maxCpu) {
        this.maxCpu = maxCpu;
    }

    public double getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(double maxMemory) {
        this.maxMemory = maxMemory;
    }

    public double getMaxDisk() {
        return maxDisk;
    }

    public void setMaxDisk(double maxDisk) {
        this.maxDisk = maxDisk;
    }

    public String toStringforPlot(){
        return "" + cpu + " " + maxCpu + " " + memory + " " + maxMemory + " " + disk  + " " + maxDisk;
    }
}
