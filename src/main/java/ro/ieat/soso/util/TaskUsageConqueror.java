package ro.ieat.soso.util;

import org.springframework.web.client.RestTemplate;
import ro.ieat.soso.core.coalitions.Usage;
import ro.ieat.soso.core.jobs.Job;
import ro.ieat.soso.core.jobs.TaskHistory;
import ro.ieat.soso.core.jobs.TaskUsage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by adrian on 11.01.2016.
 * This class is used to read data from the task_usage files describing Google cluster data usage.
 */
public class TaskUsageConqueror {

    public static RestTemplate template = new RestTemplate();

    public static void map(FileReader fileReader, Map<Long, Job> jobMap, long start, long end) throws IOException, InterruptedException {


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


            Job j = jobMap.get(jobId);
            TaskUsage task = new TaskUsage(taskIndex, jobId, j.getLogicJobName());
            ArrayList<Usage> usages = new ArrayList<Usage>();
            usages.add(usage);
            task.setUsageList(usages);
            task.setMachineId(machine);

            TaskHistory t = j.getTaskHistory().get(taskIndex);

            task.setStartTime(t.getScheduleTime());
            task.setFinishTime(t.getFinishTime());

            if(t.getTaskUsage() != null) {
                t.getTaskUsage().getUsageList().add (usage);
            }else{
                t.setTaskUsage(task);
            }
            if(t.getMachineId() == 0)
                t.setMachineId(machine);
//            String postMachineUsage = "http://localhost:8088/assign/usage/" + machine;
//            Logger.getLogger("Conquer").info(postMachineUsage);


            //machineRepository.timeJobMap.put(machineRepository.jobRepo.get(jobId).getSubmitTime(), jobId);
            jobMap.put(j.getJobId(), j);

        }
        br.close();
        fileReader.close();

    }
}
