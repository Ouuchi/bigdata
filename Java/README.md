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