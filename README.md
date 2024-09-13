# Project Setup and Instructions

## Setup and Prepare

1. **Ensure Docker is up to date.**
2. **Inside the main folder, run:**

    ```bash
    docker compose up -d
    ```

3. **After a few minutes, check the containers with:**

    ```bash
    docker ps
    ```

    You should see: `kafka`, `zookeeper`, `spark_master`, and `cassandra`.

4. **Enter the `spark_master` container:**

    ```bash
    docker exec -it spark_master bash
    apt-get install libgl1 libglib2.0-0 -y
    cd /home
    git clone https://github.com/ultralytics/yolov5.git
    cd yolov5
    pip install -r requirements.txt
    pip3 install kafka-python
    exit
    ```

5. **Enter the `kafka` container:**

    ```bash
    docker exec -it kafka bash
    kafka-topics.sh --create --topic pothole --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
    exit
    ```

6. **Enter the `cassandra` container:**

    ```bash
    docker exec -it cassandra bash
    cqlsh -u cassandra -p cassandra
    CREATE KEYSPACE ph WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
    CREATE TABLE ph.results(
        id ASCII PRIMARY KEY,
        name TEXT,
        content TEXT,
        cord_thres TEXT
    );
    ```

## Running

1. **Open a new terminal and enter the `spark_master` container:**

    ```bash
    docker exec -it spark_master bash
    cd /home/producer/
    python3 producer.py
    ```

    Keep this terminal open to monitor new images in the `producer/images` folder and push them to Kafka.

2. **Open a second terminal and copy images to the `app/producer/images` folder.** This will trigger Kafka to process the images, with output visible in the previous terminal.

3. **Open a third terminal and enter the `spark_master` container:**

    ```bash
    docker exec -it spark_master bash
    cd /home/
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 streamingKafka2Console.py
    ```

    This submits a Spark job to monitor Kafka for new messages, process images, and check for potholes. Keep this terminal open to maintain Spark monitoring.

4. **To check results in Cassandra, open a new terminal and enter the `cassandra` container:**

    ```bash
    docker exec -it cassandra bash
    cqlsh -u cassandra -p cassandra
    SELECT * FROM ph.results;
    ```
