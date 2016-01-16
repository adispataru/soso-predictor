package ro.ieat.soso.predictor;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.config.Configuration;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.core.prediction.DurationPrediction;
import ro.ieat.soso.core.prediction.MachinePrediction;
import ro.ieat.soso.persistence.JobRepository;
import ro.ieat.soso.persistence.MachineRepository;
import ro.ieat.soso.persistence.TaskUsageMappingRepository;
import ro.ieat.soso.predictor.prediction.PredictionFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by adrian on 11.12.2015.
 */

@RestController
public  class Predictor {

    @Autowired
    TaskUsageMappingRepository taskUsageMappingRepository;
    @Autowired
    JobRepository jobRepository;
    @Autowired
    MachineRepository machineRepository;

    @RequestMapping(method = RequestMethod.PUT, path = "/predict/allUsage/{historyStart}/{historyEnd}", consumes = "application/json")
    public void predictAllMachines(@PathVariable long historyStart,@PathVariable long historyEnd){
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByStartTimeGreaterThanAndFinishTimeLessThan(historyStart, historyEnd);
        for(Machine m : machineRepository.findAll()){
            List<Usage> usageList = new ArrayList<Usage>();
            long time = System.currentTimeMillis();
            for(TaskUsage taskUsage : taskUsageList){
                //Get list before to avoid repeated interogation
//            TaskUsage taskUsage = taskUsageMappingRepository.findOne(taskUsageId);
                if(taskUsage.getMachineId() != m.getId())
                    continue;

                for (Usage u : taskUsage.getUsageList()) {
                    if (u.getStartTime() < historyStart)
                        continue;
                    if (u.getEndTime() > historyEnd)
                      break;
                    usageList.add(u);
                }

            }
            MachinePrediction prediction = PredictionFactory.predictMachineUsage(usageList);
            prediction.setStartTime(historyEnd * Configuration.TIME_DIVISOR);
            prediction.setEndTime((historyEnd + Configuration.STEP) * Configuration.TIME_DIVISOR);
            m.setPrediction(prediction);
            machineRepository.save(m);
        }

    }



    @RequestMapping(method = RequestMethod.POST, path = "/predict/usage/{historyStart}/{historyEnd}", consumes = "application/json")
    public MachinePrediction predictMachineUsage(@RequestBody Machine m, @PathVariable long historyStart,
                                                 @PathVariable long historyEnd) throws IOException, InterruptedException {

//        String path = Configuration.MACHINE_USAGE_PATH + "/" + machineId;
//        Machine m = MachineUsageMapper.readOne(new File(path), historyStart, historyEnd);
        List<Long> machineUsage =  m.getTaskUsageList();
//        Logger.getLogger("Predictor").info("tasks: " + machineUsage.size());

        List<Usage> usageList = new ArrayList<Usage>();
        List<TaskUsage> taskUsageList = taskUsageMappingRepository.
                findByMachineIdAndStartTimeLessThan(m.getId(), historyEnd);
        Logger.getLogger("Predictor").info("Task usage list size " + taskUsageList.size());
        for(TaskUsage taskUsage : taskUsageList){
            //Get list before to avoid repeated interogation
//            TaskUsage taskUsage = taskUsageMappingRepository.findOne(taskUsageId);

            for (Usage u : taskUsage.getUsageList()) {
                if (u.getStartTime() < historyStart)
                    continue;
//                irrelevant
//                if (u.getEndTime() > historyEnd)
//                    continue;
                usageList.add(u);
            }

        }


        MachinePrediction prediction = PredictionFactory.predictMachineUsage(usageList);
        prediction.setStartTime(historyEnd * Configuration.TIME_DIVISOR);
        prediction.setEndTime((historyEnd + Configuration.STEP) * Configuration.TIME_DIVISOR);
        m.setPrediction(prediction);

        //Eventually this would become a call via REST
        //MachineUsagePredictionController.updateMachineStatus(m.getId(), m)

        return prediction;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/predict/job/{logicJobName}/{historyEnd}")
    public DurationPrediction predictJobRuntime(@PathVariable final String logicJobName, Long historyEnd) throws IOException {


        List<Job> jobs = jobRepository.findByLogicJobNameAndSubmitTimeLessThan(logicJobName, historyEnd);
        List<Long> durationList = new ArrayList<Long>();
        for(Job j : jobs){
            if(j.getFinishTime() == 0 || j.getScheduleTime() == 0 || !"finish".equals(j.getStatus()))
                continue;
            Long duration = j.getFinishTime() - j.getScheduleTime();
            if(duration > 0)
                durationList.add(duration);
        }

        DurationPrediction duration = PredictionFactory.predictTime(durationList);
//        if(duration != null) {
//            JobRuntimePredictionController.updateJobDuration(logicJobName, duration);
//        }
        return duration;
    }

}
