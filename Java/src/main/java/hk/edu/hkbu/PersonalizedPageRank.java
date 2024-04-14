package hk.edu.hkbu;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.util.*;

final class PersonalizedPageRank {

    private static Map<String, String> config = readConfiguration("config.ini");

    private static double alpha = Double.parseDouble(config.get("alpha"));
    private static int numIterations = Integer.parseInt(config.get("numIterations"));

    public static void main(String[] args) {

        System.out.println("PersonalizedPageRank START");
        long startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("CommonNeighborsRecommendation")
                .set("spark.executor.memory","2g");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> dataset = sparkContext.textFile(config.get("training_data_path"));

        JavaRDD<String[]> userPairs = dataset.map(line -> line.split(" "));

        JavaPairRDD<String, String> userPairsRDD = userPairs.flatMapToPair(pair -> {
            List<Tuple2<String, String>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(pair[0], pair[1]));
            return pairs.iterator();
        });

        JavaPairRDD<String, Iterable<String>> links = userPairsRDD.groupByKey();

        try {

            PrintWriter writer = new PrintWriter(new FileWriter(config.get("ppr_result_output_path")));

            /**
             * Read the test file.
             */

            int totalCompare = 0;
            int correctPredict = 0;


            // Read the facebook test file.
            JavaRDD<String> faceBookTestRDD = sparkContext.textFile(config.get("testing_data_path"));
            JavaRDD<String[]> faceBookTestRDDColumns = faceBookTestRDD.map(line -> line.split(" "));
            Map<String, Set<String>> testFriends = new HashMap<>();

            for (String[] elements: faceBookTestRDDColumns.collect()) {
                if (!testFriends.containsKey(elements[0])) {
                    testFriends.put(elements[0], new HashSet<>());
                }
                for (int i = 1; i < elements.length; i++) {
                    testFriends.get(elements[0]).add(elements[i]);
                }
            }

            // Output file
            for (Map.Entry<String, Set<String>> entry : testFriends.entrySet()) {

                String user = entry.getKey();

//                List<String> userList = userPairsRDD.lookup(user);
//                Set<String> userSet = new HashSet<>(userList);

                /**
                 * Get sorted common friends.
                 */
                List<String> recommendedFriends = calculatePPRFriends(user, links);

                writer.print(user);
                System.out.print(user +  " recommendation friends ->");
                int size = recommendedFriends.size();

                if (size == 0) {
                    writer.println("\n");
                    System.out.println("");
                    continue;
                }

                for (int j = 0; j < size; j++) {
                    totalCompare++;

                    // Compare the actual output recommended friends to the friends in test file.
                    if (testFriends.containsKey(recommendedFriends.get(j))) {
                        correctPredict++;
                    }

                    // output the common friends to output file.
                    if (j != size - 1) {
                        System.out.print(" " + recommendedFriends.get(j));
                        writer.print(" " + recommendedFriends.get(j));
                    } else {
                        System.out.println(" " + recommendedFriends.get(j));
                        writer.println(" " + recommendedFriends.get(j) + "\n");
                    }
                }

                writer.flush();
            }

            System.out.println("Total record: "
                    + totalCompare
                    + " Correct predict: "
                    + correctPredict
                    + " correct ratio: "
                    + (correctPredict / totalCompare));

            writer.println("Total record: "
                    + totalCompare
                    + " Correct predict: "
                    + correctPredict
                    + " correct ratio: "
                    + (correctPredict / totalCompare));

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;

            System.out.println("The whole program's execution time: " + executionTime + " milliseconds.");
            writer.println("The whole program's execution time: " + executionTime + " milliseconds.");

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        sparkContext.stop();
    }

    /**
     * Get Recommendation Friends.
     * @param user
     * @param links
     * @return
     */

    private static List<String> calculatePPRFriends(String user, JavaPairRDD<String, Iterable<String>> links) {

        /**
         * Initialize page rank.
         */
        JavaPairRDD<String, Double> ranks = links.mapToPair(rs -> {
            if (rs._1().equals(user)) {
                return new Tuple2<>(rs._1(), 1.0);
            } else {
                return new Tuple2<>(rs._1(), 0.0);
            }
        });

        JavaPairRDD<String, Double> contribs = null;

        /**
         * iteration
         */
        for (int current = 0; current < numIterations; current++) {
            // Calculates URL contributions to the rank of other URLs.
            contribs = links
                    .join(ranks)
                    .flatMapToPair(s -> {
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        int urlCount = Iterables.size(s._2._1);
                        for (String str: s._2._1) {
                            results.add(new Tuple2<>(str, s._2()._2 / urlCount * (1 - alpha)));
                        }
                        results.add(new Tuple2<>(s._1, alpha * s._2._2));
                        return results.iterator();
                    });

            ranks = contribs.reduceByKey((e1, e2) -> e1 + e2);
        }

        /**
         * Sort by ranks and get the friends.
         */
        List<String> frieldsToRecommand = ranks
                .mapToPair(e -> new Tuple2<>(e._2, e._1))
                .sortByKey(false)
                .values()
                .take(Integer.parseInt(config.get("recommend_k")));

        return frieldsToRecommand;
    }

    private static Map<String, String> readConfiguration(String configFile) {
        Map<String, String> configMap = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) { // Ignore empty lines and comments
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        configMap.put(key, value);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return configMap;
    }
}
