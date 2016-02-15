package ro.ieat.soso.reasoning.startegies;

import ro.ieat.soso.core.coalitions.Coalition;
import ro.ieat.soso.core.coalitions.Machine;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by adrian on 2/10/16.
 */
public class AntColonyClusteringStrategy {

    private final Map<Long, Long> machineMaxTaskMapping;
    private Map<Integer, Map<Integer, Double>> similarityMap;
    private static Long clusterCounter = 1L;
    private double alpha = 0.01;
    private double gamma = 1.0;
    private Double dropProbability = 0.1;
    private static int antCounter = 0;
    private final Logger LOG = Logger.getLogger(AntColonyClusteringStrategy.class.toString());

    public AntColonyClusteringStrategy(Map<Long, Long> machineMaxTaskMapping){
        this.machineMaxTaskMapping = machineMaxTaskMapping;
    }


    public List<Coalition> clusterize(List<Machine> machines){
        similarityMap = new TreeMap<>();
        List<Ant> ants = new ArrayList<>();
        for(Machine m : machines){
            Ant a = new Ant(m);
            ants.add(a);
        }

        LOG.info("Learning threshold");
        learnThreshold(ants);
        LOG.info("Done.");

        LOG.info("Random meeting ants: " + ants.size());
        Map<Long, List<Ant>> clusters = randomMeetAnts(ants);
        LOG.info("Done.");
        Map<Long, Double> nClusters = new TreeMap<>();
        Map<Long, Double> averageMPlus = new TreeMap<>();
        List<Ant> freeAnts = new ArrayList<>();

        LOG.info("Processing clusters..." + clusters.size());
        for(Long label : clusters.keySet()){
            Double mPlus = .0;
            for(Ant ant : clusters.get(label)){
                mPlus += ant.mPlus;
            }
            mPlus /= clusters.get(label).size();

            Double nCluster = 1.0 * clusters.get(label).size()/ ants.size();
            Double acceptanceProbability = alpha * mPlus + (1 - alpha) * nCluster;
            if(acceptanceProbability < dropProbability){
                freeAnts.addAll(clusters.get(label));
            }else {
                nClusters.put(label, nCluster);
                averageMPlus.put(label, mPlus);
            }
        }
        LOG.info("Processing free ants: " + freeAnts.size());
        for(Ant ant : freeAnts){
            ant.reset();
            Double maxSim = Double.MIN_VALUE;
            Integer maxSimAnt = 0;
            for(Integer antId : similarityMap.get(ant.id).keySet()){
                if(similarityMap.get(ant.id).get(antId) > maxSim){
                    maxSim = similarityMap.get(ant.id).get(antId);
                    maxSimAnt = antId;
                }
            }
            Long label = ants.get(maxSimAnt).label;
            if(!clusters.containsKey(label))
                clusters.put(label, new ArrayList<>());
            clusters.get(label).add(ant);
            ant.label = label;
        }
        LOG.info("Creating coalitions from clusters...");
        List<Coalition> result = new ArrayList<>();
        for(Long label : clusters.keySet()){
            if(!label.equals(0L)) {
                int size = clusters.get(label).size();
                LOG.info("Cluster " + label + "; size: " + size);
                int processed = 0;
                while (processed < size - label) {
                    Coalition c = new Coalition();
                    c.setId(0);
                    c.setConfidenceLevel(1.0);
                    List<Machine> machineList = new ArrayList<>();
                    LOG.severe("Processed label size: " + processed + " " + label + " " + size);
                    int i;
                    for (i = processed; i < processed + label; i++) {
                        machineList.add(clusters.get(label).get(i).data);
                    }
                    while (machineList.size() < label && clusters.get(0L).size() > 0){
                        machineList.add(clusters.get(0L).remove(0).data);
                        i++;
                    }

                    c.setMachines(machineList);
                    LOG.info("Coalition info: "  + c.getMachines().size());
                    result.add(c);;
                    processed += i;
                }
                while(processed < label){
                    Coalition c = new Coalition();
                    c.setId(0);
                    c.setConfidenceLevel(1.0);
                    List<Machine> machineList = new ArrayList<>();
                    LOG.severe("Processed label size: " + processed + " " + label + " " + size);
                    int i;
                    for (i = 0; i < clusters.get(label).size(); i++) {
                        machineList.add(clusters.get(label).get(i).data);
                    }
                    while (machineList.size() < label && clusters.get(0L).size() > 0){
                        machineList.add(clusters.get(0L).remove(0).data);
                        i++;
                    }

                    c.setMachines(machineList);
                    LOG.info("MAchines in coalition: " + c.getMachines().size());
                    result.add(c);;
                    processed += i;
                }

            }
        }
        for(Ant ant : clusters.get(0L)){
            Coalition c = new Coalition();
            c.setId(0);
            c.setConfidenceLevel(1.0);
            List<Machine> machineList = new ArrayList<>();
            machineList.add(ant.data);
            c.setMachines(machineList);
            result.add(c);
        }
        LOG.info("Created " + result.size() + " coalitions!");
        return result;
    }

    private Map<Long, List<Ant>> randomMeetAnts(List<Ant> ants) {
        Random r = new Random();
        int total = 1000000;
        Map<Long, List<Ant>> clusters = new TreeMap<>();
        for (int i = 0; i < total; i++){
            Integer first = r.nextInt(ants.size());
            Integer second = r.nextInt(ants.size());
            Ant firstAnt = ants.get(first);
            Ant secondAnt = ants.get(second);
            firstAnt.meetingCounter ++;
            secondAnt.meetingCounter ++;
            Double similarity = similarityMap.get(first).get(second);
            if(similarity == null){
                similarity = .0;
            }
            if(similarity > firstAnt.threshold && similarity > secondAnt.threshold){
                applyAcceptRules(firstAnt, secondAnt, clusters);
            }else{
                applyRejectRules(firstAnt, secondAnt, clusters);
            }

        }
        for(Ant a : ants){
            if(clusters.get(a.label) == null)

                clusters.put(a.label, new ArrayList<>());
            if(!clusters.get(a.label).contains(a)){
                clusters.get(a.label).add(a);
            }
        }
        return clusters;
    }

    private void applyRejectRules(Ant firstAnt, Ant secondAnt, Map<Long, List<Ant>> clusters) {
        if(!firstAnt.label.equals(0L) && secondAnt.label.equals(firstAnt.label)){
            if(firstAnt.mPlus < secondAnt.mPlus){
                firstAnt.mPlus = .0;
                firstAnt.m = .0;
                clusters.get(firstAnt.label).remove(firstAnt);
                firstAnt.label = 0L;
                secondAnt.m = increase(secondAnt.m);
                secondAnt.mPlus = decrease(secondAnt.mPlus);
            }
            else{
                secondAnt.mPlus = .0;
                secondAnt.m = .0;
                clusters.get(secondAnt.label).remove(secondAnt);
                secondAnt.label = 0L;
                firstAnt.m = increase(firstAnt.m);
                firstAnt.mPlus = decrease(firstAnt.mPlus);
            }
        }
    }

    private void applyAcceptRules(Ant firstAnt, Ant secondAnt, Map<Long, List<Ant>> clusters) {
        if(firstAnt.label.equals(0L) && secondAnt.label.equals(0L)){
            Long firstMaxHere = machineMaxTaskMapping.get(firstAnt.data.getId());
            Long secondMaxHere = machineMaxTaskMapping.get(secondAnt.data.getId());
            if(firstMaxHere == null && secondMaxHere == null)
                return;
            Long label = Math.max(firstMaxHere != null ? firstMaxHere : 0L,
                    secondMaxHere != null ? secondMaxHere : 0L);
            firstAnt.label = label;
            secondAnt.label = label;
            if(!clusters.containsKey(label)) {
                clusters.put(label, new ArrayList<>());
            }
            clusters.get(label).add(firstAnt);
            clusters.get(label).add(firstAnt);
            return;
        }
        if(firstAnt.label.equals(0L) && !secondAnt.label.equals(0L)){
            firstAnt.label = secondAnt.label;
            clusters.get(secondAnt.label).add(firstAnt);
            return;
        }
        if(!firstAnt.label.equals(0L) && secondAnt.label.equals(0L)){
            secondAnt.label = firstAnt.label;
            clusters.get(firstAnt.label).add(secondAnt);
            return;
        }
        if(!firstAnt.label.equals(0L) && !secondAnt.label.equals(0L)){
            if(firstAnt.label.equals(secondAnt.label)){
                firstAnt.m = increase(firstAnt.m);
                secondAnt.m = increase(secondAnt.m);
                firstAnt.mPlus = increase(firstAnt.mPlus);
                secondAnt.mPlus = increase(secondAnt.mPlus);
            }
            else{
                if(firstAnt.m < secondAnt.m){
                    clusters.get(firstAnt.label).remove(firstAnt);
                    firstAnt.label = secondAnt.label;
                    clusters.get(secondAnt.label).add(firstAnt);
                    firstAnt.m = decrease(firstAnt.m);
                    secondAnt.m = decrease(secondAnt.m);
                }
                else{
                    clusters.get(secondAnt.label).remove(secondAnt);
                    secondAnt.label = firstAnt.label;
                    clusters.get(firstAnt.label).add(secondAnt);
                    firstAnt.m = decrease(firstAnt.m);
                    secondAnt.m = decrease(secondAnt.m);
                }

            }
        }


    }

    public Double decrease(Double m){
        return (1 - alpha) * m;
    }

    public Double increase(Double m){
        return (1 - alpha) * m + m;
    }

    public void learnThreshold(List<Ant> ants){
        int sampleSize = 3000;

        for(int i = 0; i < ants.size(); i++){

            similarityMap.put(i, new TreeMap<>());
            Ant a = ants.get(i);
            Double maxSimilarity = Double.MIN_VALUE;
            Double average = .0;
            int index = new Random().nextInt(ants.size() - sampleSize);
            for(int j = 0; j < sampleSize; j++){
                Double similarity;

                if(similarityMap.get(j) != null && similarityMap.get(j).get(i) != null) {
                    similarity = similarityMap.get(j).get(i);
                }
                else {
                    similarity = computeSimilarity(a, ants.get(j + index));
                }
                similarityMap.get(i).put(j + index, similarity);
                average += similarity;
                if(similarity > maxSimilarity)
                    maxSimilarity = similarity;
            }
            average /= sampleSize;
            a.threshold = (maxSimilarity + average) / 2;
        }

    }

    private Double computeSimilarity(Ant a, Ant ant) {
        Double similarity = .0;
//        LOG.info(a.data.getId() + " first id.");
//        LOG.info(ant.data.getId() + " second id.");
//        LOG.info(machineMaxTaskMapping.size() + " size.");
        Long firstMax = 0L;
        if(machineMaxTaskMapping.get(a.data.getId()) != null)
            firstMax = machineMaxTaskMapping.get(a.data.getId());
        Long secondMax = 0L;
         if(machineMaxTaskMapping.get(ant.data.getId()) != null)
             secondMax = machineMaxTaskMapping.get(ant.data.getId());
        similarity = 1 - gamma * Math.abs(firstMax - secondMax)/ Math.max(firstMax, secondMax);
        return similarity;
    }


    private class Ant {
        Machine data;
        Long label;
        Double threshold;
        Long meetingCounter;
        //nest size parameter
        Double m;
        //acceptance degree
        Double mPlus;
        Integer id;

        public Ant(Machine m){
            this.id = antCounter++;
            this.data = m;
            label = 0L;
            threshold = .0;
            meetingCounter = 0L;
            this.m = .0;
            mPlus = .0;

        }

        public boolean equals(Object o){
            if(o instanceof Ant){
                Ant a = (Ant) o;
                return a.data.getId().equals(this.data.getId());
            }
            return false;
        }

        public void reset(){
            label = 0L;
        }

    }
}
