# module: kafka_topics

This is the kafka_topics module.

version: 1.0.0

## Author

- Piotr Kepka (@pkepka)

## Description

This Ansible module allows to create, list, and delete Kafka topics, as well as to set a specific amount of partitions on a specific topic and set retention on one particular topic.

## Requirements 

To use the module, please install the required libraries first. 

- requests
- certifi
- confluent-kafka

For details see: library/requirements.txt


## Options
- mode
  - description: Mode of operation of the module.
    Allowed modes:
    - create
    - list
    - delete
    - alter_retention
    - alter_num_partitions
    - show_config
  - required: **true**
  - type: str
- broker
  - description: Address of the Kafka Broker
  - required: **true**
  - type: str
  - example: localhost:9092
- topic
  - description: Name of the Kafka Topic
  - required: false (**true** for modes: *create*, *delete*, *alter_num_partitions*, *alter_retention*)
  - type: str
- num_partitions
  - description: Number of partitions of Kafka Topic
  - required: false (**true** for modes: *create*, *alter_num_partitions*)
  - default: DEFAULT_NUM_PARTITIONS
  - type: int
- replication_factor
  - description: Replication factor of Kafka Topic
  - required: false (**true** for mode: *create*)
  - default: DEFAULT_REPLICATION_FACTOR
  - type: int
- retention
  - description: Retention time (in ms) of Kafka Topic
  - required: false (**true** for mode: *alter_retention*)
  - default: DEFAULT_RETENTION
  - type: int

## Return
- msg
  - description: Contains result of task execution in case of success or error message on errors.
  - type: str
  - returned: always
  - example: 'Topic topic1 created'
- changed
  - description: Informs about the change made in the state of the host.
  - type: bool
  - returned: always
  - example: True
- failed
  - description: Informs about task execution problems.
  - type: bool
  - returned: sometimes
  - example: True
- unreachable
  - description: Informs that the broker is unreachable.
  - type: bool
  - returned: sometimes
  - example: True

## Examples

### Create a topic
```
  - name: create topic1
    kafka_topics:
      mode: 'create'
      broker: 'localhost:9092'
      topic: 'topic1'
      replication_factor: 1
      num_partitions: 3
```

### List topics
```
  - name: list kafka topics
    kafka_topics:
      mode: 'list'
      broker: 'localhost:9092'
```

### Delete a topic
```
  - name: delete topic1
    kafka_topics:
      mode: 'delete'
      broker: 'localhost:9092'
      topic: 'topic1'
```

### Set retention
```
  - name: alter retention topic1
    kafka_topics:
      mode: 'alter_retention'
      broker: 'localhost:9092'
      topic: 'topic1'
      retention: 86400000
```

