package ro.ieat.soso.reasoning.startegies;

import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;
import ro.ieat.soso.reasoning.ResourceAbstraction;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by adrian on 2/10/16.
 */
public class RankAndLabelCoalitionStrategy extends AbstractCoalitionStrategy {

    private final Map<Long, Long> machineMaxTaskMapping;
    private Map<Integer, Map<Integer, Double>> similarityMap;
    private static Long clusterCounter = 1L;
    private double alpha = 0.01;
    private double gamma = 1.0;
    private Double dropProbability = 0.1;
    private int maxSize;
    private static int antCounter = 0;
    private final Logger LOG = Logger.getLogger(RankAndLabelCoalitionStrategy.class.toString());

    public RankAndLabelCoalitionStrategy(Map<Long, Long> machineMaxTaskMapping, int maxSize){
        this.machineMaxTaskMapping = machineMaxTaskMapping;
        this.maxSize = maxSize;
    }


    public List<Coalition> createCoalitions(List<Machine> machines){
        similarityMap = new TreeMap<>();
        List<Rankable> rankableMachines = new ArrayList<>();
        for(Machine m : machines){
            Rankable a = new Rankable(m);
            rankableMachines.add(a);
        }

        LOG.info("Learning threshold");
        List<Rankable> ranked = rankLearnThreshold(rankableMachines, maxSize);
        LOG.info("Done.");

        LOG.info("Random meeting ants: " + ranked.size());
//        Map<Long, List<Ant>> clusters = randomMeetAnts(rankableMachines);
        LOG.info("Done.");


        LOG.info("Creating coalitions from clusters...");
        List<Coalition> result = new ArrayList<>();
        Iterator<Rankable> iterator = ranked.iterator();
        List<Machine> freeMachines = new ArrayList<>();
        while(iterator.hasNext()){
            double capacity = 0.0;
            Coalition c = new Coalition();
            c.setId(0);
            c.setConfidenceLevel(1.0);
            List<Machine> machineList = new ArrayList<>();
            while (capacity < 1 && iterator.hasNext()){
                Rankable rankable = iterator.next();
                Machine m = rankable.data;

                if(capacity + rankable.threshold <= 1) {
                    machineList.add(m);
                    capacity += rankable.threshold;
                }
                else{
                    freeMachines.add(m);
                    if(machineList.size() > 0) {
                        c.setMachines(machineList);
                        result.add(c);
                        LOG.info("Machines in coalition: " + c.getMachines().size());
                    }
                    break;
                }
            }

        }

        for(Machine m : freeMachines){
            Coalition c = new Coalition();
            c.setId(0);
            c.setConfidenceLevel(1.0);
            List<Machine> machineList = new ArrayList<>();
            machineList.add(m);
            c.setMachines(machineList);
            result.add(c);
        }
        LOG.info("Created " + result.size() + " coalitions!");
        return result;
    }

    @Override
    public void learnThreshold(List<? extends ResourceAbstraction> resourceAbstractionList) {

        List<Rankable> rankableList = new ArrayList<>();
        resourceAbstractionList.forEach(r ->{
            if(r instanceof Rankable)
                rankableList.add((Rankable) r);
        });

        rankLearnThreshold(rankableList, maxSize);
    }

    protected Double adjustThresholdAndValue(ResourceAbstraction a, ResourceAbstraction resourceAbstraction) {
        return adjustThresholdAndValue((Rankable) a, (Rankable) resourceAbstraction);
    }


    private List<Rankable> rankLearnThreshold(List<Rankable> resources, int maxSize){

        List<Rankable> result = new ArrayList<>();
        for (Rankable a : resources) {

            adjustThresholdAndValue(a, maxSize);
            int j = 0;
            while (j < result.size()) {
                if (a.threshold < result.get(j).threshold)
                    j++;
                else {
                    result.add(j, a);
                    break;
                }
            }
            if (j == result.size()) {
                result.add(a);
            }
        }
        return result;

    }

    private void adjustThresholdAndValue(Rankable a, int maxSize) {
        Long resourceValue = 0L;
        if(machineMaxTaskMapping.get(a.data.getId()) != null)
            resourceValue = machineMaxTaskMapping.get(a.data.getId());
        a.threshold = (maxSize - resourceValue + 1) / (double) maxSize;
        a.value = resourceValue/(double)maxSize;
    }


    private class Rankable extends ResourceAbstraction {
        Machine data;
        Long label;
        Double threshold;
        Long meetingCounter;
        //nest size parameter
        Double m;
        //acceptance degree
        Double value;
        Integer id;

        Rankable(Machine m){
            this.id = antCounter++;
            this.data = m;
            label = 0L;
            threshold = .0;

            meetingCounter = 0L;
            this.m = .0;
            this.value = .0;

        }

        public boolean equals(Object o){
            if(o instanceof Rankable){
                Rankable a = (Rankable) o;
                return a.data.getId().equals(this.data.getId());
            }
            return false;
        }

        void reset(){
            label = 0L;
        }

    }


}
