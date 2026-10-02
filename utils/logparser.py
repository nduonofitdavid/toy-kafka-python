import uuid
from model import Partition, RecordTypeEnum, TopicRecordD, RecordBatch, Record, RequestHeaderD, APIRequestBodyD, TopicD, DescRequestBodyD
from .byteparser import *

cluster_metadata_loaded = False

def parse_msg_header(request_bytes: bytes) -> RequestHeaderD:
    offset = 0
    req_api_key = int.from_bytes(request_bytes[offset:offset+2], "big") # second two bytes 
    
    offset+=2
    req_api_version = int.from_bytes(request_bytes[offset:offset+2], "big") # the next two bytes
    
    offset+=2
    correlation_id = request_bytes[offset:offset+4] # the next four bytes
    
    offset+=4
    client_id_length = int.from_bytes(request_bytes[offset:offset+2], "big")
    
    offset+=2
    client_id_content = request_bytes[offset:offset+client_id_length]
    
    offset+=client_id_length
    tag_buffer = request_bytes[offset:offset+1]
    
    offset+=1
    
    return RequestHeaderD(
        req_api_key=req_api_key, 
        req_api_version=req_api_version, 
        correlation_id=correlation_id, 
        client_id_length=client_id_length, 
        client_id_content=client_id_content, 
        tag_buffer=tag_buffer,
        offset=offset
    )
    
def parse_apiversions_msg_body(request_bytes, offset) -> APIRequestBodyD:
    
    encoded_len = request_bytes[offset]
    offset+=1
    
    actual_length = encoded_len - 1
    
    body_cc = request_bytes[offset:offset+actual_length]
    offset+=actual_length
    
    encoded_sv_len = request_bytes[offset]
    offset+=1
    
    actual_sv_len = encoded_sv_len - 1
    
    
    client_sv_cont = request_bytes[offset:offset+actual_sv_len]
    offset+=actual_sv_len
    
    tag_buffer = request_bytes[offset:]
    
    return APIRequestBodyD(
        encoded_len=encoded_len,
        body_cc=body_cc, 
        client_sv_len=encoded_sv_len, 
        client_sv_cont=client_sv_cont,
        tag_buffer=tag_buffer
    )
    
def parse_desc_tpc_part_body(request_bytes, offset) -> DescRequestBodyD:
    encoded_len = request_bytes[offset]
    offset+=1
    
    actual_arr_len = encoded_len - 1
    
    topics = [None] * actual_arr_len
    
    for i in range(actual_arr_len):
        name_length = request_bytes[offset]
        a_name_length = name_length - 1
        offset+=1
        
        name = request_bytes[offset:offset+a_name_length]
        offset+=a_name_length
        
        n_tag_buffer = request_bytes[offset]
        offset+=1
        
        topics[i] = TopicD(
            name_length=name_length,
            name=str(name, encoding='utf-8')
        )
    
    r_part_limit = int.from_bytes(request_bytes[offset:offset+4], "big")
    offset+=4
    
    cursor = request_bytes[offset]
    offset+=1
    
    tag_buffer = request_bytes[offset]
    
    return DescRequestBodyD(
        encoded_len=encoded_len,
        array_len=actual_arr_len,
        topics=topics,
        request_partition_limit=r_part_limit,
        cursor=cursor,
        tag_buffer=tag_buffer
    )

def parse_partition_record(offset: int, log_bytes: bytes) -> tuple[int, Partition]:
    # partition id
    partition_id = int.from_bytes(log_bytes[offset:offset+4], "big")
    offset+=4    
    # topic uuid
    topic_uuid = uuid.UUID(bytes=log_bytes[offset:offset+16])
    offset+=16
    # replica array len
    enc_replica_len, offset = read_unsigned_varint(log_bytes, offset)
    # indicates the number of replicas in replica array
    ac_replical_len = enc_replica_len - 1    
    replica_arrays = [0] * ac_replical_len
    
    for i in range(ac_replical_len):
        replica_arr_id = int.from_bytes(log_bytes[offset:offset+4], "big")
        replica_arrays[i] = replica_arr_id # when serializing serialize with 4 bytes
        offset+=4
    
    # in sync replica array
    enc_sync_len, offset = read_unsigned_varint(log_bytes, offset)
    ac_sync_len = enc_sync_len - 1
    sync_arr = [0] * ac_sync_len
    
    for i in range(ac_sync_len):
        sync_replica_id = int.from_bytes(log_bytes[offset:offset+4], "big")
        sync_arr[i] = sync_replica_id
        offset+=4
        
    # removing replicas array
    enc_remv_len, offset = read_unsigned_varint(log_bytes, offset)
    ac_remv_len = enc_remv_len - 1
    rmv_arr = [0] * ac_remv_len
    
    for i in range(ac_remv_len):
        rmv_replilca_id = int.from_bytes(log_bytes[offset:offset+4], "big")
        rmv_arr[i] = rmv_replilca_id
        offset+=4
        
    # add replicas aray
    
    enc_add_len, offset = read_unsigned_varint(log_bytes, offset)
    ac_add_len = enc_add_len - 1
    add_arr = [0] * ac_add_len
    
    for i in range(ac_add_len):
        add_replica_id = int.from_bytes(log_bytes[offset:offset+4], "big")
        add_arr[i] = add_replica_id
        offset+=4
        
    # leader id
    leader = int.from_bytes(log_bytes[offset:offset+4], 'big')
    offset+=4
    
    # leader epoch
    leader_epoch = int.from_bytes(log_bytes[offset:offset+4], "big")
    offset+=4
    
    # partition epoch
    _ = log_bytes[offset:offset+4]
    offset+=4
    
    # length of directors array
    enc_dir_len, offset = read_unsigned_varint(log_bytes, offset)
    ac_dir_len = enc_dir_len - 1
    dir_arr: list[uuid.UUID | None] = [None] * ac_dir_len
    
    # finish dir array
    for i in range(ac_dir_len):
        dir_id = uuid.UUID(bytes=log_bytes[offset:offset+16])
        dir_arr[i] = dir_id
        offset+=16
        
    # tagged fields
    _, offset = read_unsigned_varint(log_bytes, offset)
    
    partition_obj = Partition(
        topic_id=topic_uuid,
        error_code=0,
        partition_index=partition_id,
        leader_id=leader,
        leader_epoch=leader_epoch,
        replica_arrays=replica_arrays,
        isr_nodes=sync_arr
        )
    
    return offset, partition_obj
    
    
def parse_topic_record(offset: int, log_bytes: bytes) -> tuple[int, TopicRecordD]:
    encoded_len, offset = read_unsigned_varint(log_bytes, offset)
    actual_len = encoded_len - 1
    
    name = log_bytes[offset:offset+actual_len]
    offset+=actual_len
    topic_uuid = uuid.UUID(bytes=log_bytes[offset:offset+16])
    offset+=16
    
    # tagged fields
    _, offset = read_unsigned_varint(log_bytes, offset)
    topic_obj = TopicRecordD(name=str(name, encoding="utf-8"), id=topic_uuid)
    return offset, topic_obj
    
    
def parse_feature_record(offset: int, log_bytes: bytes) -> int:
    encoded_len, offset = read_unsigned_varint(log_bytes, offset)
    actual_len = encoded_len - 1
    
    name = log_bytes[offset:offset+actual_len]
    offset+=actual_len
    # feature level
    _ = log_bytes[offset:offset+2]
    offset+=2
    # tagged field count
    _, offset = read_unsigned_varint(log_bytes, offset)
    return offset


def read_batch(log_bytes: bytes, topic_to_uuid: dict[str, uuid.UUID], uuid_to_partitions: dict[uuid.UUID, list[Partition]], offset: int=0) -> int:
    batch_start = offset
    
    # base offset
    _ = log_bytes[offset:offset+8] # read the first 8 bytes
    offset+=8    
    # batch len
    btch_len = int.from_bytes(log_bytes[offset:offset+4], "big")
    offset+=4
    # part leader epock
    _ = log_bytes[offset:offset+4]
    offset+=4
    # magic byte
    _ = log_bytes[offset]
    offset+=1
    # crc code
    _ = log_bytes[offset:offset+4]
    offset+=4
    # attributes
    _ = log_bytes[offset:offset+2]
    offset+=2
    # last offset delta
    _ = log_bytes[offset:offset+4]
    offset+=4
    # base timestamp
    _ = log_bytes[offset:offset+8]
    offset+=8
    # max timestamp
    _ = log_bytes[offset:offset+8]
    offset+=8
    # producer_id
    _ = log_bytes[offset:offset+8]
    offset+=8
    # producer_epoch
    _ = log_bytes[offset:offset+2]
    offset+=2
    # base sequence 
    _ = log_bytes[offset:offset+4]
    offset+=4
    # records len
    record_len = int.from_bytes(log_bytes[offset:offset+4], "big")
    offset+=4
    
    records = []
    for _ in range(record_len):
        # record len
        record_len, offset = read_varint(log_bytes, offset)
        # attributes
        _ = log_bytes[offset]
        offset+=1
        # timestamp delta
        _, offset = read_varint(log_bytes, offset)
        # offset delta
        _, offset = read_varint(log_bytes, offset)
        # key length
        key_len, offset = read_varint(log_bytes, offset)

        if key_len != -1: # -1 indicates null key
            # the key
            _ = log_bytes[offset:offset+key_len]
            offset+=key_len
        
        # value len
        value_len, offset = read_varint(log_bytes, offset)
        value_start = offset
        
        # frame version
        _ = log_bytes[offset]
        offset+=1
        # type
        type = log_bytes[offset]
        offset+=1
        # version
        _ = log_bytes[offset]
        offset+=1
        
        if type == RecordTypeEnum.TOPIC_REC:
            offset, topic_obj = parse_topic_record(offset, log_bytes)
            topic_to_uuid[topic_obj.name] = topic_obj.id
        
        elif type == RecordTypeEnum.PARTITION_REC:
            offset, partition_obj = parse_partition_record(offset, log_bytes)
            uuid_to_partitions[partition_obj.topic_id].append(partition_obj)
            
        else:
            offset = parse_feature_record(offset, log_bytes)
        
        assert offset - value_start == value_len
        
         # headers array count
        _, offset = read_varint(log_bytes, offset)
    
    expected_end = batch_start + 12 + btch_len
    assert offset == expected_end  
    return offset


def read_cluster_metadata(cluster_loc: str, topic_to_uuid: dict[str, uuid.UUID], uuid_to_partitions: dict[uuid.UUID, list[Partition]]):
    global cluster_metadata_loaded
    # caching stuff, making sure you only load once.
    if cluster_metadata_loaded:
        return 
    
    with open(cluster_loc, 'rb') as file:
        log_bytes: bytes = file.read()
        byte_count: int = len(log_bytes)
        offset = read_batch(log_bytes, topic_to_uuid, uuid_to_partitions)
        
        while offset != byte_count:
            offset = read_batch(log_bytes, topic_to_uuid, uuid_to_partitions, offset=offset)        

    cluster_metadata_loaded = True

def read_record_batch(req_bytes: bytes, offset: int) -> tuple[RecordBatch, int]:
    base_offset = int.from_bytes(req_bytes[offset:offset+8], "big")
    offset+=8
    batch_size = int.from_bytes(req_bytes[offset:offset+4], "big")
    offset+=4
    part_leader_epoch = int.from_bytes(req_bytes[offset:offset+4], "big")
    offset+=4
    magic_byte = req_bytes[offset]
    offset+=1
    crc32 = int.from_bytes(req_bytes[offset:offset+4], "big")
    offset+=4
    attributes = int.from_bytes(req_bytes[offset:offset+2], "big")
    offset+=2
    last_offset_delta = int.from_bytes(req_bytes[offset:offset+4], "big")
    offset+=4
    first_timestamp = int.from_bytes(req_bytes[offset:offset+8], "big")
    offset+=8
    last_timestamp = int.from_bytes(req_bytes[offset:offset+8], "big")
    offset+=8
    producer_id = int.from_bytes(req_bytes[offset:offset+8], "big")
    offset+=8
    producer_epoch = int.from_bytes(req_bytes[offset:offset+2], "big")
    offset+=2
    base_sequence = int.from_bytes(req_bytes[offset:offset+4], "big")
    offset+=4
    records_arr_len = int.from_bytes(req_bytes[offset:offset+4], "big")
    offset+=4
    
    records_arr: list[Record] = [None] * records_arr_len # type: ignore
    
    for i in range(records_arr_len):
        record_size, offset = read_varint(req_bytes, offset)
        attributes = req_bytes[offset]
        offset+=1
        
        timestamp_delta, offset = read_varint(req_bytes, offset)
        offset_delta, offset = read_varint(req_bytes, offset)
        key_length, offset = read_varint(req_bytes, offset)
        
        if key_length != -1:
            # we ignore keys for now since not provided in challenge, refactor later
            # key = str(req_bytes[offset:offset+key_length], encoding="utf-8")
            # offset += key_length
            ...
        
        value_length, offset = read_varint(req_bytes, offset)
        value = req_bytes[offset:offset+value_length]
        offset+=value_length
        
        headers_count, offset = read_varint(req_bytes, offset)
        
        rec = Record(
            record_size=record_size,
            attributes=attributes,
            timestamp_delta=timestamp_delta,
            offset_delta=offset_delta,
            key_length=key_length,
            value_length=value_length,
            value=str(value, encoding="utf-8"),
            headers_count=headers_count
        )
        
        records_arr[i] = rec
    
    rec_batch_obj = RecordBatch(
        base_offset=base_offset,
        batch_size=batch_size,
        part_leader_epoch=part_leader_epoch,
        magic_byte=magic_byte,
        crc_32=crc32,
        attributes=attributes,
        last_offset_delta=last_offset_delta,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        base_sequence=base_sequence,
        records_array=records_arr
    )
    
    return rec_batch_obj, offset
                
def parse_produce_req_v11(req_bytes: bytes, offset: int) -> tuple[dict[str, list[int]], dict[tuple[str, int], list[RecordBatch]]]:
    topic_to_partid: dict[str, list[int]] = defaultdict(list)
    partid_to_recbatch: dict[tuple[str, int], list[RecordBatch]] = defaultdict(list)
    
    # transactional id
    _, offset = read_unsigned_varint(req_bytes, offset)
    # required ACKs
    _ = req_bytes[offset:offset+2]
    offset+=2
    # timeout
    _ = req_bytes[offset:offset+4]
    offset+=4
    # topic array length
    enc_topic_arr_len, offset = read_unsigned_varint(req_bytes, offset)
    ac_topic_arr_len = enc_topic_arr_len - 1
    
    # reading each topic
    for _ in range(ac_topic_arr_len):
        enc_topic_name_len, offset = read_unsigned_varint(req_bytes, offset)
        ac_topic_name_len = enc_topic_name_len - 1
        
        topic_name = req_bytes[offset:offset+ac_topic_name_len]
        offset+= ac_topic_name_len
        
        # partition array length
        enc_part_arr_len, offset = read_unsigned_varint(req_bytes, offset)
        ac_part_arr_len = enc_part_arr_len - 1
        
        part_ids = [0] * ac_part_arr_len
        # reading partitions array
        for j in range(ac_part_arr_len):
            # partition index
            part_index = int.from_bytes(req_bytes[offset:offset+4], "big")
            offset+=4
            part_ids[j] = part_index
            
            # record batch
            record_batch_size, offset = read_unsigned_varint(req_bytes, offset)
            
            rec_batch_start = offset
            while (offset - rec_batch_start) < record_batch_size:
                record_batch, offset = read_record_batch(req_bytes, offset)
                partid_to_recbatch[(str(topic_name, encoding="utf-8"), part_index)].append(record_batch)

            # tag buffer   
            _ = req_bytes[offset]
            offset+=1
            
        topic_to_partid[str(topic_name, encoding="utf-8")] = part_ids
        
        # tag buffer
        _ = req_bytes[offset]
        offset+=1
            
    # tag buffer
    _ = req_bytes[offset]
    offset+=1
    return topic_to_partid, partid_to_recbatch