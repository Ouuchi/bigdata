package hk.edu.hkbu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.*;
import java.util.*;

public class CommonFriends {

    private static Map<String, String> config = readConfiguration("config.ini");

    public static void main(String[] args) {

        System.out.println("CommonFriends START");

        long startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("CommonNeighborsRecommendation")
                .set("spark.executor.memory","4g");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        /**
         * Read the file and convert it to a user pair RDD.
         */
        JavaPairRDD userPairsRDD = sparkContext
                .textFile(config.get("training_data_path"))
                .map(line -> line.split(" "))
                .flatMapToPair(pair -> {
                    List<Tuple2<String, String>> pairs = new ArrayList<>();
                    pairs.add(new Tuple2<>(pair[0], pair[1]));
                    return pairs.iterator();
                });
        /**
         * Group by key.
         */
        JavaPairRDD<String, Iterable<String>> userFriends = userPairsRDD.groupByKey();

        /**
         * Calculate common friends RDD.
         */
        JavaPairRDD<Tuple2<String, String>, Iterable<String>> commonFriends = userFriends
            .flatMapToPair(pair -> {
                List<Tuple2<Tuple2<String, String>, Iterable<String>>> result = new ArrayList<>();
                String user20 = pair._1();
                Iterable<String> userFriends2 = pair._2();
                Set<String> commonFrields = new HashSet<>();
                for (String friend : userFriends2) {
                    for (String friend2 : userFriends2) {
                        if (!friend.equals(friend2)) {
                            Tuple2<String, String> friendPair = new Tuple2<>(friend, friend2);
                            commonFrields.add(user20);
                            result.add(new Tuple2<>(friendPair, commonFrields));
                            friendPair = new Tuple2<>(friend2, friend);
                            result.add(new Tuple2<>(friendPair, commonFrields));
                        }
                    }
                    Tuple2<String, String> friendPair = new Tuple2<>(user20, friend);
                    result.add(new Tuple2<>(friendPair, new HashSet<>()));
                    friendPair = new Tuple2<>(friend, user20);
                    result.add(new Tuple2<>(friendPair, new HashSet<>()));
                }
                return result.iterator();
            }).reduceByKey((friends1, friends2) -> {

                Set<String> common = new HashSet<>();
                friends1.forEach(e -> {
                    common.add(e);
                });

                friends2.forEach(e2 -> {
                    common.add(e2);
                });

                return common;
            }).filter(pair3 -> pair3._2.iterator().hasNext());
        commonFriends.cache();
        try {

            PrintWriter writer = new PrintWriter(new FileWriter(config.get("common_friends_result_output_path")));

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;
            String execution_time = "Execution time of finding common friends: " + executionTime + " milliseconds.";
            System.out.println(execution_time);

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
                List<String> recommendedFriends = commonFriends
                        .filter(pair -> pair._1()._1().equals(user) || pair._1()._2().equals(user))
                        .flatMap(pair -> pair._2().iterator())
                        .mapToPair(friend -> new Tuple2<>(friend, 1))
                        .reduceByKey((count1, count2) -> count1 + count2)
                        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                        .sortByKey(false)
                        .values()
                        .take(Integer.parseInt(config.get("recommend_k")));

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
                        System.out.print(" " + recommendedFriends.get(j) );
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

            endTime = System.currentTimeMillis();
            executionTime = endTime - startTime;

            writer.println(execution_time);
            System.out.println("The whole program's execution time: " + executionTime + " milliseconds.");
            writer.println("The whole program's execution time: " + executionTime + " milliseconds.");

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        sparkContext.close();
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