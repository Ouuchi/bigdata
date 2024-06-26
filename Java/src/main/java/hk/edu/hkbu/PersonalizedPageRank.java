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
                .setAppName("PersonalizedPageRank")
                .set("spark.executor.memory","4g");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> dataset = sparkContext.textFile(config.get("training_data_file"));

        JavaRDD<String[]> userPairs = dataset.map(line -> line.split(" "));

        JavaPairRDD<String, String> userPairsRDD = userPairs.flatMapToPair(pair -> {
            List<Tuple2<String, String>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(pair[0], pair[1]));
            return pairs.iterator();
        });

        JavaPairRDD<String, Iterable<String>> links = userPairsRDD.groupByKey();

        try {
            String filePath = config.get("result_output_path") + "ppr_result_" + config.get("recommend_k") + ".txt";
            PrintWriter writer = new PrintWriter(new FileWriter(filePath));

            /**
             * Read the test file.
             */
            int testTotal = 0;
            int predTotal = 0;
            int correctPredict = 0;

            long totalRunningTime = 0;
            int userCount = 0;

            /**
             * Read the facebook test file.
             */
            JavaRDD<String> faceBookTestRDD = sparkContext.textFile(config.get("testing_data_file"));
            JavaRDD<String[]> faceBookTestRDDColumns = faceBookTestRDD.map(line -> line.split(" "));
            Map<String, Set<String>> testFriends = new HashMap<>();

            for (String[] elements: faceBookTestRDDColumns.collect()) {
                if (!testFriends.containsKey(elements[0])) {
                    testFriends.put(elements[0], new HashSet<>());
                }
                for (int i = 1; i < elements.length; i++) {
                    testTotal++;
                    testFriends.get(elements[0]).add(elements[i]);
                }
            }
            Map<String, Set<String>> predictCorrectMap = new HashMap<>();

            // Output file
            for (Map.Entry<String, Set<String>> entry : testFriends.entrySet()) {

                String user = entry.getKey();

                /**
                 * Get sorted common friends.
                 */
                long startRunningTime = System.currentTimeMillis();
                List<String> recommendedFriends = calculatePPRFriends(user, links);
                long endRunningTime = System.currentTimeMillis();
                totalRunningTime = totalRunningTime +  (endRunningTime - startRunningTime);

                writer.print(user);
                System.out.print(user +  " recommendation friends ->");
                int size = recommendedFriends.size();
                predTotal+= size;

                if (size == 0) {
                    writer.println();
                    System.out.println("");
                    continue;
                }

                for (int j = 0; j < size; j++) {

                    // Compare the actual output recommended friends to the friends in test file.
                    if (testFriends.get(user).contains(recommendedFriends.get(j))) {
                        predictCorrectMap.put(user, testFriends.get(user));
                        correctPredict++;
                    }

                    // output the common friends to output file.
                    if (j != size - 1) {
                        System.out.print(" " + recommendedFriends.get(j));
                        writer.print(" " + recommendedFriends.get(j));
                    } else {
                        System.out.println(" " + recommendedFriends.get(j));
                        writer.println(" " + recommendedFriends.get(j));
                    }
                }

                writer.flush();
                userCount++;
            }

            double precision = 0;
            double recall = 0;
            if (predTotal != 0)
                precision = (double) correctPredict / predTotal * 100;

            if (testTotal != 0)
                recall = (double) correctPredict / testTotal * 100;

            System.out.println("Evaluation: "
                    + testTotal
                    + ", predTotal: "
                    + predTotal
                    + ", correctPredict: "
                    + correctPredict
                    + ", precisions: "
                    + precision
                    + "%, recalls: "
                    + recall
                    + "%");

            writer.println("## Evaluation: "
                    + testTotal
                    + ", predTotal: "
                    + predTotal
                    + ", correctPredict: "
                    + correctPredict
                    + ", precisions: "
                    + precision
                    + "%, recalls: "
                    + recall
                    + "%");

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            long averageRunningTime = totalRunningTime / userCount;
            System.out.println("The average running times for each target user : " + (double) averageRunningTime / 1000  + " seconds.");
            System.out.println("The whole program's execution time: " + (double) executionTime/1000  + " seconds.");
            writer.println("## The average running times for each target user : " + (double) averageRunningTime / 1000 + " seconds.");
            writer.println("## The whole program's execution time: " + (double) executionTime/1000 + " seconds.");

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        sparkContext.stop();
    }

    /**
     *
     * @param user
     * @param links
     * @return
     */
    private static List<String> calculatePPRFriends(String user, JavaPairRDD<String, Iterable<String>> links) {

//
//        JavaRDD<String> existingFriends = links
//                .filter(entry -> entry._1.equals(user))
//                .values()
//                .flatMap(entry -> {
//                    Set<String> list = new HashSet<>();
//                    for (String str: entry) {
//                        list.add(str);
//                    }
//                    return list.iterator();
//                });

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
