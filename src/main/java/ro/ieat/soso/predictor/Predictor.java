package ro.ieat.soso.predictor;


import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.mappers.JobReader;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.core.prediction.MachinePrediction;
import ro.ieat.soso.predictor.persistence.MachineRepository;
import ro.ieat.soso.predictor.prediction.PredictionFactory;
import ro.ieat.soso.reasoning.controllers.JobRuntimePredictionController;
import ro.ieat.soso.reasoning.controllers.MachineUsagePredictionController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by adrian on 11.12.2015.
 */
public  class Predictor {

    public static int predictMachineUsage(long machineId, long historyStart, long historyEnd) throws IOException, InterruptedException {

//        String path = Configuration.MACHINE_USAGE_PATH + "/" + machineId;
//        Machine m = MachineUsageMapper.readOne(new File(path), historyStart, historyEnd);
        Machine m = MachineRepository.findOne(machineId);
        List<TaskUsage> machineUsage =  m.getTaskUsageList();

        List<Usage> usageList = new ArrayList<Usage>();
        for(TaskUsage taskUsage : machineUsage){
            for(Usage u : taskUsage.getUsageList()){
                if(u.getStartTime() < historyStart)
                    continue;
                if(u.getEndTime() > historyEnd)
                    continue;
                usageList.add(u);
            }
        }


        MachinePrediction prediction = PredictionFactory.predictMachineUsage(usageList);
        prediction.setStartTime(historyEnd * Configuration.TIME_DIVISOR);
        prediction.setEndTime((historyEnd + Configuration.STEP) * Configuration.TIME_DIVISOR);
        m.setPrediction(prediction);

        //Eventually this would become a call via REST
        MachineUsagePredictionController.updateMachineStatus(m.getId(), m);

        return 0;
    }

    public static int predictJobRuntime(String logicJobName, long historyStart, long historyEnd) throws IOException {

        String jobsPath = "./data/s_jobs.csv";

        List<Job> jobs = JobReader.getJobsWithLogicJobName(jobsPath, logicJobName, historyStart, historyEnd);
        List<Long> durationList = new ArrayList<Long>();
        for(Job j : jobs){
            if(j.getFinishTime() == 0 || j.getScheduleTime() == 0 || !"finish".equals(j.getStatus()))
                continue;
            Long duration = j.getFinishTime() - j.getScheduleTime();
            if(duration > 0)
                durationList.add(duration);
        }

        DurationPrediction duration = PredictionFactory.predictTime(durationList);
        if(duration != null) {
            JobRuntimePredictionController.updateJobDuration(logicJobName, duration);
        }
        return 0;
    }

}
