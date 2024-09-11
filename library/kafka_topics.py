#!/usr/bin/python

from __future__ import (absolute_import, division, print_function)
from confluent_kafka.admin import (AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource,
                                   AclBinding, AclBindingFilter, ResourceType, ResourcePatternType,
                                   AclOperation, AclPermissionType, KafkaException, KafkaError)
from confluent_kafka import KafkaException
import sys
import threading
import logging
__metaclass__ = type

logging.basicConfig()

DOCUMENTATION = r'''
---
module: kafka_topics

short_description: This is the kafka_topics module.

version_added: "1.0.0"

description: This Ansible module allows to create, list, and delete Kafka topics, as well as to set a specific amount of partitions on a specific topic and set retention on one particular topic.

options:
    mode:
        description: Mode of operation of the module
            Allowed modes:
                - create
                - list
                - delete
                - alter_retention
                - alter_num_partitions
                - show_config
        required: true
        type: str
    broker:
        description: Address of the Kafka Broker
        required: true
        type: str
        example: localhost:9092
    topic:
        description: Name of the Kafka Topic
        required: false (true for modes: create, delete, alter_num_partitions, alter_retention)
        type: str
    num_partitions:
        description: Number of partitions of Kafka Topic
        required: false (true for modes: create, alter_num_partitions)
        default: DEFAULT_NUM_PARTITIONS
        type: int
    replication_factor:
        description: Replication factor of Kafka Topic
        required: false (true for mode: create)
        default: DEFAULT_REPLICATION_FACTOR
        type: int
    retention:
        description: Retention time (in ms) of Kafka Topic
        required: false (true for mode: alter_retention)
        default: DEFAULT_RETENTION
        type: int

author:
    - Piotr Kepka (@pkepka)
'''

EXAMPLES = r'''
# Create a topic
  - name: create topic1
    kafka_topics:
      mode: 'create'
      broker: 'localhost:9092'
      topic: 'topic1'
      replication_factor: 1
      num_partitions: 3

# List topics
  - name: list kafka topics
    kafka_topics:
      mode: 'list'
      broker: 'localhost:9092'

# Delete a topic
  - name: delete topic1
    kafka_topics:
      mode: 'delete'
      broker: 'localhost:9092'
      topic: 'topic1'

# Set retention
  - name: alter retention topic1
    kafka_topics:
      mode: 'alter_retention'
      broker: 'localhost:9092'
      topic: 'topic1'
      retention: 86400000
'''

RETURN = r'''
msg:
    description: Contains result of task execution in case of success or error message on errors.
    type: str
    returned: always
    example: 'Topic topic1 created'
changed:
    description: Informs about the change made in the state of the host.
    type: bool
    returned: always
    example: True
failed:
    description: Informs about task execution problems.
    type: bool
    returned: sometimes
    example: True
unreachable:
    description: Informs that the broker is unreachable.
    type: bool
    returned: sometimes
    example: True
'''

from ansible.module_utils.basic import AnsibleModule

# for connection to Kafka Broker
SOCKET_TIMEOUT_MS = 30000

# from Kafka documentation
MAX_NUM_PARTITIONS = 200000

DEFAULT_NUM_PARTITIONS = 3
DEFAULT_REPLICATION_FACTOR = 1
# from Kafka documentation
DEFAULT_RETENTION = 604800000

MODE_CREATE = 'create'
MODE_LIST = 'list'
MODE_DELETE = 'delete'
MODE_ALTER_RETENTION = 'alter_retention'
MODE_ALTER_NUM_PARTITIONS = 'alter_num_partitions'
MODE_SHOW_CONFIG = 'show_config'

PARAM_MODE = 'mode'
PARAM_BROKER = 'broker'
PARAM_TOPIC = 'topic'
PARAM_NUM_PARTITIONS = 'num_partitions'
PARAM_REPLICATION_FACTOR = 'replication_factor'
PARAM_RETENTION = 'retention'

RET_CHANGED = 'changed'
RET_FAILED = 'failed'
RET_UNREACHABLE = 'unreachable'
RET_MSG = 'msg'

def create_topics(a, topics, num_partitions, replication_factor, result):
    """ Create topics """

    new_topics = [NewTopic(topic, num_partitions, replication_factor) for topic in topics]
    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(new_topics)

    message = []
    changed = False
    failed = False
    unreachable = False
    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0
    # to the create_topics() call.
    # All futures will finish at the same time.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            message.append("Topic {} created".format(topic))
            changed = True
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                message.append("Topic {} already exists".format(topic))
            elif e.args[0].code() == KafkaError._TIMED_OUT:
                message.append("Broker is unreachable")
                unreachable = True
                break
            else:
                message.append("Failed to create topic {}: {}".format(topic, e))
                failed = True
        except Exception as e:
            message.append("Failed to create topic {}: {}".format(topic, e))
            failed = True

    result[RET_CHANGED] = changed
    result[RET_FAILED] = failed
    result[RET_UNREACHABLE] = unreachable
    result[RET_MSG] = message


def list_topics(a, result):
    """ list topics """

    message = []
    changed = False
    failed = False
    unreachable = False
    try:
        md = a.list_topics(timeout=10)

        message.append(" {} topics:".format(len(md.topics)))
        for t in iter(md.topics.values()):

            if len(message) > 0:
                message.append("")
            
            if t.error is not None:
                errstr = ": {}".format(t.error)
            else:
                errstr = ""

            message.append("  '{}' with {} partition(s){}".format(t, len(t.partitions), errstr))

            for p in iter(t.partitions.values()):
                if p.error is not None:
                    errstr = ": {}".format(p.error)
                else:
                    errstr = ""

                message.append("partition {} leader: {}, replicas: {},"
                        " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
                                                    p.isrs, errstr))

    except KafkaException as e:
        if e.args[0].code() == KafkaError._TIMED_OUT or e.args[0].code() == KafkaError._TRANSPORT:
            message.append("Broker is unreachable")
            unreachable = True
        else:
            message.append("{}".format(e))
            failed = True
    except Exception as e:
        message.append("{}".format(e))
        failed = True

    result[RET_CHANGED] = changed
    result[RET_FAILED] = failed
    result[RET_UNREACHABLE] = unreachable
    result[RET_MSG] = message
 

def delete_topics(a, topics, result):
    """ delete topics """

    # Call delete_topics to asynchronously delete topics, a future is returned.
    # By default this operation on the broker returns immediately while
    # topics are deleted in the background. But here we give it some time (30s)
    # to propagate in the cluster before returning.
    #
    # Returns a dict of <topic,future>.
    fs = a.delete_topics(topics, operation_timeout=30)

    message = []
    changed = False
    failed = False
    unreachable = False
    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            message.append("Topic {} deleted".format(topic))
            changed = True
        except KafkaException as e:
            if e.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                message.append("Topic {} does not exist".format(topic))
            elif e.args[0].code() == KafkaError._TIMED_OUT:
                message.append("Broker is unreachable")
                unreachable = True
                break
            else:
                message.append("Failed to delete topic {}: {}".format(topic, e))
                failed = True
        except Exception as e:
            message.append("Failed to delete topic {}: {}".format(topic, e))
            failed = True

    result[RET_CHANGED] = changed
    result[RET_FAILED] = failed
    result[RET_UNREACHABLE] = unreachable
    result[RET_MSG] = message


def alter_topics_retention(a, topics, retention, result):
    """ alter topics retention """

    if len(topics) < 1 or len(topics) > 1:
        result[RET_CHANGED] = False
        result[RET_FAILED] = True
        result[RET_MSG] = 'Only one topic at the time is allowed'
    elif len(topics[0]) == 0:
        result[RET_CHANGED] = False
        result[RET_FAILED] = True
        result[RET_MSG] = 'Empty topic is not allowed'
    else:
        topic_str = topics[0]
        retention_str = 'retention.ms={}'.format(retention)
        
        params = ['TOPIC', topic_str, retention_str]

        alter_configs(a, params, result)


def alter_topics_num_partitions(a, topics, num_partitions, result):
    """ alter topics number of partitions """

    message = []
    message.append('If you want to change the number of partitions or replicas of your Kafka topic,')
    message.append('you can use a streaming transformation to automatically stream all of the messages')
    message.append('from the original topic into a new Kafka topic that has the desired number')
    message.append('of partitions or replicas.')
    message.append('')
    message.append('More:')
    message.append('https://developer.confluent.io/tutorials/change-topic-partitions-replicas/ksql.html')
    message.append('')    

    result[RET_CHANGED] = False
    result[RET_FAILED] = True
    result[RET_UNREACHABLE] = False
    result[RET_MSG] = message


def show_topics_config(a, topics, result):
    """ show topics configuration """

    if len(topics) < 1 or len(topics) > 1:
        result[RET_CHANGED] = False
        result[RET_FAILED] = True
        result[RET_MSG] = 'Only one topic at the time is allowed'
    elif len(topics[0]) == 0:
        result[RET_CHANGED] = False
        result[RET_FAILED] = True
        result[RET_MSG] = 'Empty topic is not allowed'
    else:
        topic_str = topics[0]
        params = ['TOPIC', topic_str]

        describe_configs(a, params, result)


def describe_configs(a, args, result):
    """ describe configs """

    resources = [ConfigResource(restype, resname) for
                 restype, resname in zip(args[0::2], args[1::2])]

    fs = a.describe_configs(resources)

    message = []
    changed = False
    failed = False
    unreachable = False
    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            configs = f.result()
            for config in iter(configs.values()):
                message.append(print_config(config, 1, []))

        except KafkaException as e:
            if e.args[0].code() == KafkaError._TIMED_OUT:
                message.append("Broker is unreachable")
                unreachable = True
            else:
                message.append("Failed to describe {}: {}".format(res, e))
                failed = True
        except Exception:
            failed = True
            raise

    result[RET_CHANGED] = changed
    result[RET_FAILED] = failed
    result[RET_UNREACHABLE] = unreachable
    result[RET_MSG] = message


def alter_configs(a, args, result):
    """ Alter configs atomically, replacing non-specified
    configuration properties with their default values.
    """

    message = []
    changed = False
    failed = False
    unreachable = False
    resources = []
    for restype, resname, configs in zip(args[0::3], args[1::3], args[2::3]):
        resource = ConfigResource(restype, resname)
        resources.append(resource)
        for k, v in [conf.split('=') for conf in configs.split(',')]:
            resource.set_config(k, v)

    fs = a.alter_configs(resources)

    # Wait for operation to finish.
    for res, f in fs.items():
        try:
            f.result()  # empty, but raises exception on failure
            message.append("{} configuration successfully altered".format(res))
            changed = True
        except KafkaException as e:
            if e.args[0].code() == KafkaError._TIMED_OUT:
                message.append("Broker is unreachable")
                unreachable = True
            else:
                failed = True
                raise
        except Exception:
            failed = True
            raise

    result[RET_CHANGED] = changed
    result[RET_FAILED] = failed
    result[RET_UNREACHABLE] = unreachable
    result[RET_MSG] = message


def print_config(config, depth, message):
    message.append('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
          ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
           config.is_read_only, config.is_default,
           config.is_sensitive, config.is_synonym,
           ["%s:%s" % (x.name, ConfigSource(x.source))
            for x in iter(config.synonyms.values())]))
    return message

def get_num_partitions(module):
    num_partitions = DEFAULT_NUM_PARTITIONS
    if module.params[PARAM_NUM_PARTITIONS]:
        if module.params[PARAM_NUM_PARTITIONS] > 0 and module.params[PARAM_NUM_PARTITIONS] <= MAX_NUM_PARTITIONS:
            num_partitions = module.params[PARAM_NUM_PARTITIONS]
        else:
            errMsg = "Number of partitions must be between 0 and {} (got: {})".format(MAX_NUM_PARTITIONS, module.params[PARAM_NUM_PARTITIONS])
            module.fail_json(msg=errMsg)
    return num_partitions

def get_replication_factor(module):
    replication_factor = DEFAULT_REPLICATION_FACTOR
    if module.params[PARAM_REPLICATION_FACTOR]:
        if module.params[PARAM_REPLICATION_FACTOR] > 0:
            replication_factor = module.params[PARAM_REPLICATION_FACTOR]
        else:
            errMsg = "Replication factor must be bigger than 0 (got: {})".format( module.params[PARAM_REPLICATION_FACTOR])
            module.fail_json(msg=errMsg)
    return replication_factor

def get_retention(module):
    retention = DEFAULT_RETENTION
    if module.params[PARAM_RETENTION]:
        if module.params[PARAM_RETENTION] >= -1:
            retention = module.params[PARAM_RETENTION]
        else:
            errMsg = "Retention time must be equal or bigger than -1 (got: {})".format(module.params[PARAM_RETENTION])
            module.fail_json(msg=errMsg)
    return retention


def run_module():
    # define available arguments/parameters a user can pass to the module
    module_args = dict()
    module_args[PARAM_MODE]=dict(type='str', required=True)
    module_args[PARAM_BROKER]=dict(type='str', required=True)
    module_args[PARAM_TOPIC]=dict(type='list', required=False)
    module_args[PARAM_NUM_PARTITIONS]=dict(type='int', required=False)
    module_args[PARAM_REPLICATION_FACTOR]=dict(type='int', required=False)
    module_args[PARAM_RETENTION]=dict(type='int', required=False)

    # seed the result dict in the object
    result = dict()
    result[RET_CHANGED]=False
    result[RET_FAILED]=False
    result[RET_MSG]=''

    # the AnsibleModule object will be our abstraction working with Ansible
    # this includes instantiation, a couple of common attr would be the
    # args/params passed to the execution, as well as if the module
    # supports check mode
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)


    broker = module.params[PARAM_BROKER]
    # Create Admin client
    a = AdminClient({'bootstrap.servers': broker, 'socket.timeout.ms': SOCKET_TIMEOUT_MS})

    if module.params[PARAM_MODE] == MODE_CREATE:
        num_partitions = get_num_partitions(module)
        replication_factor = get_replication_factor(module)
        create_topics(a, module.params[PARAM_TOPIC], num_partitions, replication_factor, result)

    elif module.params[PARAM_MODE] == MODE_LIST:
        list_topics(a, result)

    elif module.params[PARAM_MODE] == MODE_DELETE:
        delete_topics(a, module.params[PARAM_TOPIC], result)

    elif module.params[PARAM_MODE] == MODE_ALTER_RETENTION:
        retention=None
        if module.params[PARAM_RETENTION]:
            retention = get_retention(module)
        alter_topics_retention(a, module.params[PARAM_TOPIC], retention, result)

    elif module.params[PARAM_MODE] == MODE_ALTER_NUM_PARTITIONS:
        num_partitions=None
        if module.params[PARAM_NUM_PARTITIONS]:
            num_partitions = get_num_partitions(module)
        alter_topics_num_partitions(a, module.params[PARAM_TOPIC], num_partitions, result)

    elif module.params[PARAM_MODE] == MODE_SHOW_CONFIG:
        show_topics_config(a, module.params[PARAM_TOPIC], result)



    # the broker being unreachable
    if result[RET_UNREACHABLE] == True:
        result.pop(RET_FAILED, None)
        module.exit_json(**result)

    # something went wrong
    if result[RET_FAILED] == True:
        result.pop(RET_FAILED, None)
        result.pop(RET_UNREACHABLE, None)
        module.fail_json(**result)

    # successful execution
    result.pop(RET_FAILED, None)
    result.pop(RET_UNREACHABLE, None)
    module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()

