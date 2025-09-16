# kafka-event-logger

Basic event logger with kafka consumer and producer. Sends Kafka messages produces by using the logger in combination with the producer in main.go. Messages can then be received by the consumer, they get logged into separate files depending on the log level

## Config

Basic configuration for topics, kafka adress, partitions etc. can be found in config.yaml.

## How to run:

To run simply check out the repository and start the dependencies. All the dependencies like kafka broker are contained in the docker-compose.yml file:

```
docker-compose up # -d flag for detached
```

After docker-compose is running execute

```
go mod tidy
go run .
```
