package ro.ieat.soso.util;

import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.core.jobs.TaskUsage;
import ro.ieat.soso.predictor.persistence.MachineRepository;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by adrian on 11.01.2016.
 * This class is used to read data from the task_usage files describing Google cluster data usage.
 */
public class TaskUsageConqueror {

    public static void map(FileReader fileReader, MachineRepository machineRepository, long start, long end) throws IOException, InterruptedException {


        BufferedReader br = new BufferedReader(fileReader);

        long startTime;
        long endTime;
        long jobId;
        long taskIndex;
        long machine;


        double cpu;
        double mem;
        double disk = .0;
        double maxCpu = .0;
        double maxMemory = .0;

        double maxDisk = .0;

        String[] tokens;
        for(String line; (line = br.readLine()) != null; ) {
            tokens = line.split(",");
            startTime = Long.parseLong(tokens[0]);
            endTime = Long.parseLong(tokens[1]);
            if(startTime <   start * 1000000)
                continue;
            if(endTime > end * 1000000)
                return;

            jobId = Long.parseLong(tokens[2]);
            taskIndex = Long.parseLong(tokens[3]);
            machine = Long.parseLong(tokens[4]);


            cpu = Double.parseDouble(tokens[5]);
            mem = Double.parseDouble(tokens[6]);
            if(tokens.length > 9) {
                maxMemory = Double.parseDouble(tokens[10]);
                disk = Double.parseDouble(tokens[12]);
                maxCpu = Double.parseDouble(tokens[13]);


                maxDisk = .0;
                if (tokens[14].length() > 0)
                    maxDisk = Double.parseDouble(tokens[14]);

            }

            Usage usage = new Usage(startTime, endTime, cpu, mem, disk);
            usage.setMaxCpu(maxCpu);
            usage.setMaxMemory(maxMemory);
            usage.setMaxDisk(maxDisk);


            TaskUsage task = new TaskUsage(taskIndex, jobId, machineRepository.jobRepo.get(jobId).getLogicJobName());
            ArrayList<Usage> usages = new ArrayList<Usage>();
            usages.add(usage);
            task.setUsageList(usages);

            TaskHistory t = machineRepository.jobRepo.get(jobId).getTaskHistory().get(taskIndex);
            if(t.getTaskIndex() == taskIndex)
                if(t.getTaskUsage() != null) {
                    t.getTaskUsage().combineUsage(task);
                }else{
                    t.setTaskUsage(task);
                }

            Machine m = machineRepository.findOne(machine);
            for(int i = 0; i < m.getTaskUsageList().size(); i++){
                if (m.getTaskUsageList().get(i).getTaskIndex() == t.getTaskIndex())
                    m.getTaskUsageList().set(i, t.getTaskUsage());
            }
            machineRepository.save(m);



        }
        br.close();
        fileReader.close();

    }
}
