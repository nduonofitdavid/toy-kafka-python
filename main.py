import socket  
import threading
import uuid
from collections import defaultdict

import os

from .model import (
    APIVersionsResponse, 
    API_KEY_VERSIONS, 
    TOTAL_API_KEY_BC, 
    API_KEYS_LEN,
    Topic,
    ErrorCodes,
    DescribeTopicPartitionsResponse,
    Partition,
    ProducePartRes,
    ProduceTopicRes,
    ProduceResponseV11
)

from .utils.logparser import read_cluster_metadata, parse_produce_req_v11, parse_msg_header, parse_desc_tpc_part_body

# SOME CONSTANTS
MSG_SIZE=4
API_VERSIONS = (0, 1, 2, 3, 4)

# hard coded for now, dynamic later
CLUSTER_LOG_FILE = os.getenv("CLUSTER_LOG_PATH")
LOG_DIR = os.getenv("LOG_DIR")

if not all([CLUSTER_LOG_FILE, LOG_DIR]):
    raise Exception("Incomplete environment variables.")

def recv_exact(soc: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = soc.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Client disconnected")
        data.extend(chunk)
    return bytes(data)
    
def connection_instance(conn, address):
    try:
        while True:
            packet_size: int = int.from_bytes(recv_exact(conn, 4), "big")
            body = recv_exact(conn, packet_size)
                
            header = parse_msg_header(body)
                
            
            match header.req_api_key:
                case 18:
                    error_code = 0
                    if header.req_api_version < 0 or header.req_api_version > 4:
                        error_code = 35
                        response = (
                            (0).to_bytes(4, "big")
                            + header.correlation_id
                            + error_code.to_bytes(2, "big")
                        )
                        conn.sendall(response)
            
                    else: 
                        response_obj = APIVersionsResponse(
                            correlation_id=header.correlation_id,
                            error_code=error_code,
                            api_keys_length=API_KEYS_LEN,
                            api_keys=API_KEY_VERSIONS,
                            api_keys_bc=TOTAL_API_KEY_BC,
                            throttle_time=0
                        )
                        response = response_obj.generate_response()
                            
                        print(response.hex())
                        conn.sendall(response)
                    
                case 75:
                    parsed_body = parse_desc_tpc_part_body(body, header.offset)
                    
                    topic_to_uuid: dict[str, uuid.UUID] = {}
                    uuid_to_partition: dict[uuid.UUID, list[Partition]] = defaultdict(list)
                    
                    # reading the cluster metadata log
                    read_cluster_metadata(
                        cluster_loc=CLUSTER_LOG_FILE,
                        topic_to_uuid=topic_to_uuid,
                        uuid_to_partitions=uuid_to_partition
                    )
                    
                    req_topics = sorted([topic.name for topic in parsed_body.topics])
                    topics: list[Topic] = [None] * len(req_topics) # type: ignore
                    
                    for i in range(len(req_topics)):
                        topic = req_topics[i]
                        if topic in topic_to_uuid:
                            topic_uuid = topic_to_uuid[topic]
                            topic_partitions = uuid_to_partition[topic_uuid]
                            
                            topics[i] = Topic(
                                topic_id=topic_uuid,
                                name=topic,
                                is_internal=False,
                                partitions=topic_partitions,
                                error_code=ErrorCodes.NO_ERROR
                            )
                        
                        else:
                            topics[i] = Topic(
                                name=topic, 
                                partitions=[], 
                                is_internal=False, 
                                error_code=ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION
                            )
                    
                    response = DescribeTopicPartitionsResponse(
                        correlation_id=header.correlation_id,
                        throttle_time=0,
                        topics_array=topics, # type: ignore (we ignore the none error, because we create n topics regardless)
                        next_cursor=-1
                    )
                    
                    response = response.generate_response()
                    msg_len = len(response)
                    print(msg_len)
                    print(header.correlation_id.hex())
                    
                    response = (
                        msg_len.to_bytes(4, "big")
                        + response
                    )
                    
                    print(response.hex())
                    conn.sendall(response)
                    
                case 0:
                    # parsing the request body to obtain two dicts
                    # the topic to partition ids, and the partition id to record batches
                    topic_to_uuid: dict[str, uuid.UUID] = {}
                    uuid_to_partition: dict[uuid.UUID, list[Partition]] = defaultdict(list)
                    
                    # reading the cluster metadata log
                    read_cluster_metadata(
                        cluster_loc=CLUSTER_LOG_FILE,
                        topic_to_uuid=topic_to_uuid,
                        uuid_to_partitions=uuid_to_partition
                    )
                    
                    topic_to_partid, partid_to_recbatch = parse_produce_req_v11(body, header.offset)
                    
                    topics_response: list[ProduceTopicRes] = [None] * len(topic_to_partid) # type: ignore we still add n topics either way
                    
                    topic_index = 0
                    for topic in topic_to_partid.keys():
                        if topic in topic_to_uuid:
                            topic_uuid = topic_to_uuid[topic]
                            
                            partition_indexs = {
                                partition.partition_index for partition in uuid_to_partition[topic_uuid]
                            }
                            
                            partition_ids_req = topic_to_partid[topic]
                            partition_ids_req_len = len(partition_ids_req)
                            
                            partitions_res_obj_arr: list[ProducePartRes] = [None] * partition_ids_req_len # type: ignore we add n partitions either way so we wont get none
                            
                            for i in range(partition_ids_req_len):
                                partition_id = partition_ids_req[i]
                                if partition_id in partition_indexs:
                                    record_batches = partid_to_recbatch[(topic, partition_id)]
                                    
                                    partition_dir = os.path.join(LOG_DIR, f"{topic}-{partition_id}")
                                    os.makedirs(partition_dir, exist_ok=True)
                                    
                                    log_dir = os.path.join(partition_dir,'00000000000000000000.log')
                                    
                                    with open(log_dir, 'ab+') as file:
                                        record_batch_bytes = b"".join([record_batch.byte_representation for record_batch in record_batches])
                                        file.write(record_batch_bytes)
                                    
                                    partitions_res_obj_arr[i] = ProducePartRes(
                                            partition_id=partition_id,
                                            error_code=ErrorCodes.NO_ERROR,
                                            base_offset=0,
                                            log_append_time=-1,
                                            log_start_offset=0
                                        )
                                    
                                else:
                                    partitions_res_obj_arr[i] = ProducePartRes(
                                            partition_id=partition_id,
                                            error_code=ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION,
                                            base_offset=-1,
                                            log_append_time=-1,
                                            log_start_offset=-1
                                        )
                            
                            topics_response[topic_index] = ProduceTopicRes(
                                    topic_name=topic,
                                    partitions_arr=partitions_res_obj_arr
                                )
                            
                        else:
                            partition_ids_req = topic_to_partid[topic]
                            partitions_res_obj_arr: list[ProducePartRes] = [None] * len(partition_ids_req) # type: ignore
                            
                            for i in range(len(partition_ids_req)):
                                partition_id = partition_ids_req[i]
                                partitions_res_obj_arr[i] = ProducePartRes(
                                        partition_id=partition_id,
                                        error_code=ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION,
                                        base_offset=-1,
                                        log_append_time=-1,
                                        log_start_offset=-1
                                    )
                                
                            topics_response[topic_index] = ProduceTopicRes(
                                    topic_name=topic,
                                    partitions_arr=partitions_res_obj_arr
                                )
                        
                        topic_index+=1
                    
                    response = ProduceResponseV11(
                        correlation_id=int.from_bytes(header.correlation_id, "big"),
                        topic_response=topics_response,
                        throttle_time=0
                    )
                    
                    response = response.generate_response()
                    message_len = len(response).to_bytes(4, "big")
                    response = (
                        message_len
                        + response
                    )
                    
                    conn.sendall(response)
                    
                case _:
                    response = (
                        (0).to_bytes(4, "big")
                        + header.correlation_id
                    )
                    conn.sendall(response)
                
    except ConnectionError:
        pass
    
    finally:
        conn.close()

def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        conn, address = server.accept() # wait for client
        threading.Thread(target=connection_instance, args=(conn, address, ), daemon=True).start()
if __name__ == "__main__":
    main()