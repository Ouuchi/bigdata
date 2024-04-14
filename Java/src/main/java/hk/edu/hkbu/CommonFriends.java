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
                .setAppName("CommonFriends")
                .set("spark.executor.memory","8g");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        /**
         * Read the file and convert it to a user pair RDD.
         */
        JavaPairRDD<String, String> userPairsRDD = sparkContext
                .textFile(config.get("training_data_file"))
                .map(line -> line.split(" "))
                .flatMapToPair(pair -> {
                    List<Tuple2<String, String>> pairs = new ArrayList<>();
                    pairs.add(new Tuple2<>(pair[0], pair[1]));
                    return pairs.iterator();
                });
        userPairsRDD.cache();

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
                    String tmpUser = pair._1();

                    Iterable<String> userFriends2 = pair._2();
                    Set<String> commonFrields = new HashSet<>();
                    commonFrields.add(tmpUser);

                    for (String friend : userFriends2) {
                        for (String friend2 : userFriends2) {
                            if (!friend.equals(friend2)) {
                                Tuple2<String, String> friendPair = new Tuple2<>(friend, friend2);
                                result.add(new Tuple2<>(friendPair, commonFrields));
                                friendPair = new Tuple2<>(friend2, friend);
                                result.add(new Tuple2<>(friendPair, commonFrields));
                            }
                        }
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
                });
        commonFriends.cache();

//        List<Tuple2<Tuple2<String, String>, Iterable<String>>> list1 = commonFriends.collect();
//
//        JavaPairRDD<Tuple2<String, String>, Iterable<String>> existingsFriends = userFriends
//                 .flatMapToPair(pair -> {
//                     List<Tuple2<Tuple2<String, String>, Iterable<String>>> result = new ArrayList<>();
//                     String tmpUser = pair._1();
//                     Iterable<String> userFriends2 = pair._2();
//                     Set<String> commonFrields = new HashSet<>();
//                     for (String friend : userFriends2) {
//                         Tuple2<String, String> friendPair = new Tuple2<>(friend, tmpUser);
//                         result.add(new Tuple2<>(friendPair, commonFrields));
//                         friendPair = new Tuple2<>(tmpUser, friend);
//                         result.add(new Tuple2<>(friendPair, commonFrields));
//                     }
//                     return result.iterator();
//                 }).reduceByKey((friends1, friends2) -> {
//                    Set<String> common = new HashSet<>();
//                    return common;
//                });
//
//        List<Tuple2<Tuple2<String, String>, Iterable<String>>> list2 = existingsFriends.collect();
//
//        JavaPairRDD<Tuple2<String, String>, Iterable<String>> commonFriends2
//                = commonFriends.subtractByKey(existingsFriends);
//
//        commonFriends2.cache();
//
//        List<Tuple2<Tuple2<String, String>, Iterable<String>>> list3 = commonFriends2.collect();

        try {

            String filePath = config.get("result_output_path") + "cf_result_" + config.get("recommend_k") + ".txt";
            PrintWriter writer = new PrintWriter(new FileWriter(filePath));

            long endTime;
            long executionTime;

            /**
             * Read the test file.
             */

            int testTotal = 0;
            int predTotal = 0;
            int correctPredict = 0;
            long totalRunningTime = 0;
            int userCount = 0;


            // Read the facebook test file.
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

            // Output file
            for (Map.Entry<String, Set<String>> entry : testFriends.entrySet()) {

                String user = entry.getKey();

                long startRunningTime = System.currentTimeMillis();

                JavaRDD<String> existingFriends = userPairsRDD
                        .filter(entry2 -> entry2._1.equals(user))
                        .map((entry3) -> entry3._2).distinct();
                /**
                 * Get sorted common friends.
                 */
                List<String> recommendedFriends = commonFriends
                        .filter(pair -> pair._1()._1().equals(user))
                        .flatMap(pair -> pair._2().iterator())
                        .subtract(existingFriends)
                        .mapToPair(friend -> new Tuple2<>(friend, 1))
                        .reduceByKey((count1, count2) -> count1 + count2)
                        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                        .sortByKey(false)
                        .values()
                        .take(Integer.parseInt(config.get("recommend_k")));

                long endRunningTime = System.currentTimeMillis();
                totalRunningTime = totalRunningTime +  (endRunningTime - startRunningTime);

                writer.print(user);
                System.out.print(user +  " recommendation friends ->");
                int size = recommendedFriends.size();
                predTotal += size;

                if (size == 0) {
                    writer.println("\n");
                    System.out.println("");
                    continue;
                }

                Map<String, Set<String>> predictCorrectMap = new HashMap<>();

                for (int j = 0; j < size; j++) {

                    // Compare the actual output recommended friends to the friends in test file.
                    if (testFriends.get(user).contains(recommendedFriends.get(j))) {
                        predictCorrectMap.put(user, testFriends.get(user));
                        correctPredict++;
                    }

                    // output the common friends to output file.
                    if (j != size - 1) {
                        System.out.print(" " + recommendedFriends.get(j) );
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

            writer.println("## Evaluation:"
                    + " testTotal: "
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

            endTime = System.currentTimeMillis();
            executionTime = endTime - startTime;
            long averageRunningTime = totalRunningTime / userCount;

            System.out.println("The average running times for each target user : " + (double) averageRunningTime / 1000 + " seconds.");
            System.out.println("The whole program's execution time: " + (double) executionTime / 1000 + " seconds.");
            writer.println("## The average running times for each target user : " + (double) averageRunningTime / 1000 + " seconds.");
            writer.println("## The whole program's execution time: " + (double) executionTime / 1000 + " seconds.");

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