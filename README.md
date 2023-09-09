# Flink tests


## install azure fs plugin 
```bash 
mkdir ./plugins/azure-fs-hadoop
cp ./opt/flink-azure-fs-hadoop-1.17.1.jar ./plugins/azure-fs-hadoop
```

## start flink 
```bash
./bin/start-cluster.sh
```

## stop flink
```bash 
./bin/stop-cluster.sh
```


## start Kafka cluster
```bash
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties
```

## create Kafka topic
```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "flink" --partitions 2 --replication-factor 1
```

## push message to Kafka topic
```bash
./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic flink 

apple,banana,cherry,date,elderberry,fig,grape,honeydew,kiwi,
lemon,mango,nectarine,orange,papaya,quince,raspberry,strawberry,
tangerine,ugli,vanilla,watermelon,xigua,yam,zucchini

```

## check kafka consumer group
```bash
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group 
```

## start azure blob emulator

### configure hosts
```properties
127.0.0.1 devstoreaccount1.blob.localhost
```
### configure flink 
```properties
fs.azure.account.key.devstoreaccount1.blob.localhost: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

```properties
fs.azure.account.auth.type: OAuth
fs.azure.account.oauth.provider.type: org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.id: xxx,
fs.azure.account.oauth2.client.secret: xxxx,
fs.azure.account.oauth2.client.endpoint:  https://login.microsoftonline.com/{tenant}/oauth2/token
```
```bash
docker run -p 80:10000 -p 10000:10000 -p 10001:10001 -p 10002:10002 \
    -v $(pwd)/azurite:/data  mcr.microsoft.com/azure-storage/azurite
```

## run flink : run [OPTIONS] <jar-file> <arguments>
```bash 
./bin/flink run --class com.garmes.flink.kafka.Kafka2FileApp ./target/flink-1.0-SNAPSHOT-jar-with-dependencies.jar 

./bin/flink run --class com.garmes.flink.wordcount.WordCount ./target/flink-1.0-SNAPSHOT-jar-with-dependencies.jar 
```
