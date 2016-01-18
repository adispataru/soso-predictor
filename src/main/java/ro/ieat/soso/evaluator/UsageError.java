package ro.ieat.soso.evaluator;

import ro.ieat.soso.core.jobs.TaskUsage;

/**
 * Created by adrian on 18.01.2016.
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
}
