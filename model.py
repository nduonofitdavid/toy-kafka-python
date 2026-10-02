from dataclasses import dataclass
import uuid
from enum import IntEnum
from utils.byteparser import encode_unsigned_varint, encode_varint


# enums
class ErrorCodes(IntEnum):
    NO_ERROR=0
    UNKNOWN_TOPIC_OR_PARTITION=3

class RecordTypeEnum(IntEnum):
    FEATURE_LEVEL_REC = 12
    TOPIC_REC = 2
    PARTITION_REC = 3
    
    
# classes
class APIVersion:
    def __init__(self, api_key: int, min_support: int, max_support: int, about: str):
        self.api_key = api_key.to_bytes(2, "big")
        self.min_support = min_support.to_bytes(2, "big")
        self.max_support = max_support.to_bytes(2, "big")
        self.about = about
        
        self.tag = (0).to_bytes(1, "big")
        
        self.total_bytes = 2 + 2 + 2 + 1 # add the byte representation of the fields
        
    @property
    def byte_representation(self):
        return (
            self.api_key +
            self.min_support +
            self.max_support +
            self.tag
        )
        
class APIVersionsResponse:
    def __init__(self, correlation_id: bytes, error_code: int, api_keys_length: int, api_keys: list[APIVersion], api_keys_bc: int, throttle_time: int,) -> None:
        self.correlation_id = correlation_id
        self.error_code = error_code.to_bytes(2, "big")
        self.api_keys_length = api_keys_length.to_bytes(1, "big")
        self.api_keys = api_keys
        self.api_keys_bc = api_keys_bc
        self.throttle_time = throttle_time.to_bytes(4, "big")
        self.tag = (0).to_bytes(1, "big")
        
        self.msg_size: int = 4 + 2 + 1 + 4 + 1 + self.api_keys_bc # the number bytes used so far
        
    def generate_response(self):
        response = (
            self.msg_size.to_bytes(4, "big")
            + self.correlation_id
            + self.error_code
            + self.api_keys_length
            + b''.join([version.byte_representation for version in self.api_keys])
            + self.throttle_time
            +self.tag
        )
        return response

class Partition:
    def __init__(self, topic_id: uuid.UUID, error_code: int, partition_index: int, leader_id: int, leader_epoch: int, replica_arrays: list[int], isr_nodes: list[int],) -> None:
        self.topic_id = topic_id
        self.error_code = error_code
        self.partition_index = partition_index
        self.leader_id = leader_id
        self.leader_epoch = leader_epoch
        self.replica_array = replica_arrays
        self.isr_nodes = isr_nodes
        
        # make zero as instructed
        self.eligible_leader_rep = 1 # single byte
        self.last_known_elr = 1 # single byte
        self.offline_replicas = 1 # single byte
        self.tag_buffer = 0 # single byte
        self.topic_authorized_operations = 0 # represent this using 4 bytes
        self.next_cursor = -1
    
    
    @property
    def replica_rep(self):
        r_len = encode_unsigned_varint(len(self.replica_array) + 1)
        serialized_ids = b"".join([ replica_id.to_bytes(4, "big") for replica_id in self.replica_array])
        return r_len + serialized_ids
    
    @property
    def isr_rep(self):
        isr_len = encode_unsigned_varint(len(self.isr_nodes) + 1)
        serialized_ids = b"".join([isr_id.to_bytes(4, "big") for isr_id in self.isr_nodes])
        return isr_len + serialized_ids
    
    
    @property
    def byte_representation(self) -> bytes:
        tag_buffer_bytes = self.tag_buffer.to_bytes(1, "big")
        return (
            self.error_code.to_bytes(2, "big")
            + self.partition_index.to_bytes(4, "big")
            + self.leader_id.to_bytes(4, "big")
            + self.leader_epoch.to_bytes(4, "big")
            + self.replica_rep
            + self.isr_rep
            + encode_unsigned_varint(self.eligible_leader_rep)
            + encode_unsigned_varint(self.last_known_elr)
            + encode_unsigned_varint(self.offline_replicas)
            + tag_buffer_bytes
            + self.topic_authorized_operations.to_bytes(4, "big")
            + tag_buffer_bytes
            + self.next_cursor.to_bytes(1, "big", signed=True)
            + tag_buffer_bytes
        )

class Topic:
    def __init__(self, name: str, is_internal: bool, error_code: int, partitions: list[Partition], topic_id: uuid.UUID=uuid.UUID(int=0)) -> None:
        self.topic_id = topic_id # 16 bytes to represent this.
        self.name = name
        self.name_len = len(self.name) + 1
        self.is_internal = is_internal # 1 byte
        self.partitions = partitions # for now encode length with 1 byte
        self.topic_authorized_operations = 0 # int32, encode as 4 bytes
        self.tag_buffer = 0 # encode as a single byte
        self.error_code = error_code
        
        
    def _get_partitions(self):
        """
        If there are partitions down the line, we will serialize
        it directly from here.
        """
        
        if self.partitions:
            response = encode_unsigned_varint(len(self.partitions) + 1) + b"".join([partition.byte_representation for partition in self.partitions])
            return response
        else:
            return (1).to_bytes(1, "big")
    
    @property
    def byte_representation(self):
            
        return (
            self.error_code.to_bytes(2, "big")
            + encode_unsigned_varint(self.name_len)
            + self.name.encode("utf-8")
            + self.topic_id.bytes
            + self.is_internal.to_bytes(1, "big")
            + self._get_partitions()
            + self.topic_authorized_operations.to_bytes(4, "big")
            + self.tag_buffer.to_bytes(1, "big")
        )
        
        
        
class DescribeTopicPartitionsResponse:
    def __init__(self, correlation_id: bytes, throttle_time: int, topics_array: list[Topic], next_cursor: int,):
        self.correlation_id = correlation_id
        self.throttle_time = throttle_time
        self.topics_array = topics_array
        self.next_cursor = next_cursor
        self.tag_buffer = 0
        
    @property
    def topic_arr_len(self):
        return encode_unsigned_varint(len(self.topics_array) + 1)
    
    def generate_response(self):
        return (
            self.correlation_id
            + b"\x00"
            + self.throttle_time.to_bytes(4, "big")
            + self.topic_arr_len
            + b"".join([topic.byte_representation for topic in self.topics_array])
            + self.next_cursor.to_bytes(1, "big", signed=True)
            + self.tag_buffer.to_bytes(1, "big")
        )
          

class Record:
    def __init__(self, record_size: int, attributes: int, timestamp_delta: int, offset_delta: int, key_length: int, value_length: int, value: str, headers_count: int, key: str | None = None,) -> None:
        self.record_size = record_size
        self.attributes = attributes
        self.timestamp_delta = timestamp_delta
        self.offset_delta = offset_delta
        self.key_length = key_length
        self.key = key
        self.value_length = value_length
        self.value = value
        self.headers_count = headers_count
    
    @property
    def byte_representation(self):
        return (
            encode_varint(self.record_size)
            + self.attributes.to_bytes(1, "big")
            + encode_varint(self.timestamp_delta)
            + encode_varint(self.offset_delta)
            + encode_varint(self.key_length)
            + b"" if not self.key else self.key.encode("utf-8")
            + encode_varint(self.value_length)
            + self.value.encode("utf-8")
            + encode_varint(self.headers_count) # we always assume there are no headers for now.
        )
    

class RecordBatch:
    def __init__(self, base_offset: int, batch_size: int, part_leader_epoch: int, magic_byte: int, crc_32: int, attributes: int, last_offset_delta: int, first_timestamp: int, last_timestamp: int, producer_id: int, producer_epoch: int, base_sequence: int, records_array: list[Record],) -> None:
        self.base_offset = base_offset
        self.batch_size = batch_size
        self.part_leader_epoch = part_leader_epoch
        self.magic_byte = magic_byte
        self.crc_32 = crc_32
        self.attributes = attributes
        self.last_offset_delta = last_offset_delta
        self.first_timestamp = first_timestamp
        self.last_timestamp = last_timestamp
        self.producer_id = producer_id
        self.producer_epoch = producer_epoch
        self.base_sequence = base_sequence
        self.records_array = records_array

    @property
    def byte_representation(self):
        return (
            self.base_offset.to_bytes(8, "big")
            + self.batch_size.to_bytes(4, "big")
            + self.part_leader_epoch.to_bytes(4, "big")
            + self.magic_byte.to_bytes(1, "big")
            + self.crc_32.to_bytes(4, "big")
            + self.attributes.to_bytes(2, "big")
            + self.last_offset_delta.to_bytes(4, "big")
            + self.first_timestamp.to_bytes(8, "big")
            + self.last_timestamp.to_bytes(8, "big")
            + self.producer_id.to_bytes(8, "big")
            + self.producer_epoch.to_bytes(2, "big")
            + self.base_sequence.to_bytes(4, "big")
            + len(self.records_array).to_bytes(4, "big")
            + b"".join([record.byte_representation for record in self.records_array])
        )

class ProduceRecRes:
    ...    
 
class ProducePartRes:
    def __init__(self, partition_id: int, error_code: int, base_offset: int, log_append_time: int, log_start_offset: int, error_msg: str | None = None, records_err: list[ProduceRecRes] | None = None)-> None:
        self.partition_id = partition_id
        self.error_code = error_code
        self.base_offset = base_offset
        self.log_append_time = log_append_time
        self.log_start_offset = log_start_offset
        self.records_err = records_err
        self.error_msg = error_msg
        self.tag_buffer = 0
        
    def _get_records_error(self) -> bytes:
        # for now we will still return the same thing
        # will change when more information arises about the records error
        return (1).to_bytes(1, "big")
    
    def _get_error_msg(self) -> bytes:
        # so we allowed string to be None because it uses less memory compared to an empty string
        if self.error_msg:
            return (
                b"" + encode_unsigned_varint((len(self.error_msg) + 1))
                + self.error_msg.encode(encoding="utf-8")
            )
        
        return (1).to_bytes(1, "big")
    
    @property    
    def byte_representation(self) -> bytes:
        return (
            self.partition_id.to_bytes(4, "big")
            + self.error_code.to_bytes(2, "big")
            + self.base_offset.to_bytes(8, "big")
            + self.log_append_time.to_bytes(8, "big")
            + self.log_start_offset.to_bytes(8, "big")
            + self._get_records_error()
            + self._get_error_msg()
            + self.tag_buffer.to_bytes(1, "big")
        )

class ProduceTopicRes:
    def __init__(self, topic_name: str, partitions_arr: list[ProducePartRes]) -> None:
        self.topic_name = topic_name
        self.partitions_arr = partitions_arr
        
    def _get_topic_name(self):
        return (
            encode_unsigned_varint(len(self.topic_name) + 1)
            + self.topic_name.encode(encoding="utf-8")
        )
        
    @property
    def byte_representation(self):
        return (
            self._get_topic_name()
            + b"".join([partition.byte_representation for partition in self.partitions_arr])
        )
        

class ProduceResponseV11:
    def __init__(self, correlation_id: int, topic_response: list[ProduceTopicRes], throttle_time: int):
        self.correlation_id = correlation_id
        self.topic_response = topic_response
        self.throttle_time = throttle_time
        self.tag_buffer = 0
        
    def _get_topic_response(self) -> bytes:
        return (
            encode_unsigned_varint(len(self.topic_response) + 1)
            + b"".join([topic.byte_representation for topic in self.topic_response])
        )
                
    def generate_response(self) -> bytes:
        return (
            self.correlation_id.to_bytes(4, "big")
            + self.tag_buffer.to_bytes(1, "big")
            + self._get_topic_response()
            + self.throttle_time.to_bytes(4, "big")
            + self.tag_buffer.to_bytes(1, "big")
        )
        
# data classes

@dataclass
class TopicD:
    name_length: int
    name: str
    tag_buffer: int = 0

@dataclass
class TopicRecordD:
    name: str
    id: uuid.UUID

@dataclass
class RequestHeaderD:
    req_api_key: int
    req_api_version: int
    correlation_id: bytes
    client_id_length: int
    client_id_content: bytes
    tag_buffer: bytes
    offset: int
    
@dataclass
class APIRequestBodyD:
    encoded_len: int
    body_cc: bytes
    client_sv_len: int
    client_sv_cont: bytes
    tag_buffer: bytes
    
@dataclass
class DescRequestBodyD:
    encoded_len: int
    array_len: int
    topics: list[TopicD]
    request_partition_limit: int
    cursor: bytes
    tag_buffer: bytes
    

# instantiations
    
    
fetch = APIVersion(api_key=17, min_support=0, max_support=4, about="")

apiversions = APIVersion(api_key=18, min_support=0, max_support=4, about="Returns information about all the apis and their versions")

desc_tpic_part = APIVersion(api_key=75, min_support=0, max_support=0, about="Returns metadata about topics and their partitions")

produce_api = APIVersion(api_key=0, min_support=0, max_support=11, about="Allows sending streams of data to the topics in the kafka cluster")

api_versions_arr = [fetch, apiversions, desc_tpic_part, produce_api]

def calculate_total_bytes(versions_array: list[APIVersion]):
    total = 0
    for version in versions_array:
        total += version.total_bytes
        
    return total

TOTAL_API_KEY_BC = calculate_total_bytes(api_versions_arr)
API_KEYS_LEN = len(api_versions_arr) + 1 # because 0 is a null array and 1 is an empty array
API_KEY_VERSIONS = api_versions_arr