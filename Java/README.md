## How to build and run

```
mvn clean package
```

### Common Friends

```
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -jar target/7930-1.0-jar-with-dependencies.jar
```

### Personalised PageRank

```
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -jar target/7930-1.0-jar-with-dependencies.jar PPR
```

### How to use different training and testing file to run.

You can edit the config.ini file.

#### Specify path pointing to the training data file
training_data_file=split2/facebook_train.txt

#### Specify path pointing to the testing data file
testing_data_file=split2/facebook_test.txt

#### Specify a folder ended with / where the result will output.
result_output_path=split2/

#### The alpha used in PPR.
alpha=0.2

#### The number of iteration used in PPR.
numIterations=15

#### The number of users to recommend.
recommend_k=1