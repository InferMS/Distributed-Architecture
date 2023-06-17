import threading
import time
import random
from typing import Dict, Union, List
import logging
import grpc
import sched

from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        self.storage: Dict[int, str] = {}

    def get(self, key: int) -> Union[str, None]:
        return self.storage.get(key)

    def l_pop(self, key: int) -> Union[str, None]:
        v = self.storage.get(key)
        if v is None:
            return None
        self.storage.update({key: v[1:]})
        return v[:1]

    def r_pop(self, key: int) -> Union[str, None]:
        v = self.storage.get(key)
        if v is None:
            return None
        self.storage.update({key: v[:-1]})
        return v[-1:]

    def put(self, key: int, value: str):
        self.storage[key] = value

    def append(self, key: int, value: str):
        self.storage[key] = self.storage.get(key, '') + value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        touples = []
        for k in range(lower_val, upper_val):
            v = self.get(k)
            if v is None:
                continue
            touples.append(
                KeyValue(
                    key=k,
                    value=v
                )
            )
            self.storage.pop(k)

        dest_server_channel = grpc.insecure_channel(destination_server)
        KVStoreStub(dest_server_channel).Transfer(
            TransferRequest(keys_values=touples)
        )

    def transfer(self, keys_values: List[KeyValue]):
        for touple in keys_values:
            self.storage[touple.key] = touple.value


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        self.notify_set = []
        self.others_set = []

    def l_pop(self, key: int) -> str:
        return super().l_pop(key)

    def r_pop(self, key: int) -> str:
        return super().r_pop(key)

    def put(self, key: int, value: str):
        super().put(
            key=key,
            value=value
        )
        for replica in self.notify_set:
            KVStoreStub(grpc.insecure_channel(replica)).Put(PutRequest(
                key=key,
                value=value
            ))

    def append(self, key: int, value: str):
        super().append(
            key=key,
            value=value
        )
        for replica in self.notify_set:
            KVStoreStub(grpc.insecure_channel(replica)).Append(AppendRequest(
                key=key,
                value=value
            ))

    def add_replica(self, server: str):
        if self.consistency_level > len(self.notify_set):
            self.notify_set.append(server)
        else:
            self.others_set.append(server)


        self.__forwardAllData(server)

    def __forwardAllData(self, server: str):
        touples = []
        for k, v in self.storage.items():
            touples.append(
                KeyValue(
                    key=k,
                    value=v
                )
            )
        # print(f"server: {server}, tuplas: {touples}")
        if touples:
            dest_server_channel = grpc.insecure_channel(server)
            print(f"intentando conectar al server {server}")
            KVStoreStub(dest_server_channel).Transfer(
                TransferRequest(keys_values=touples)
            )

    def remove_replica(self, server: str):
        if server in self.notify_set:
            self.notify_set.remove(server)
            self.notify_set.append(self.others_set.pop())
        elif server in self.others_set:
            self.others_set.remove(server)
        else:
            print("server a borrar no existe")

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role

        scheduler = sched.scheduler(time.monotonic,
                                    time.sleep)

        def eventual_consistency():
            if self.others_set:
                for unconsistent_replica in self.others_set:
                    self.__forwardAllData(unconsistent_replica)

        def run_Event():
            eventual_consistency()
            scheduler.enter(EVENTUAL_CONSISTENCY_INTERVAL, 1, run_Event)

        if role == 0:
            scheduler.enter(EVENTUAL_CONSISTENCY_INTERVAL, 1, run_Event)
            scheduler.run()


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        self.kv_lock = threading.Lock()

    def Get(self, request: GetRequest, context) -> GetResponse:
        self.kv_lock.acquire()
        response = GetResponse(value=self.storage_service.get(
            key=request.key
        ))

        self.kv_lock.release()
        return response

    def LPop(self, request: GetRequest, context) -> GetResponse:
        self.kv_lock.acquire()
        response = GetResponse(value=self.storage_service.l_pop(
            key=request.key
        ))
        self.kv_lock.release()
        return response

    def RPop(self, request: GetRequest, context) -> GetResponse:
        self.kv_lock.acquire()
        response = GetResponse(value=self.storage_service.r_pop(
            key=request.key
        ))
        self.kv_lock.release()
        return response

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.kv_lock.acquire()
        self.storage_service.put(
            key=request.key,
            value=request.value)
        self.kv_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.kv_lock.acquire()
        self.storage_service.append(
            key=request.key,
            value=request.value
        )
        self.kv_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.kv_lock.acquire()
        self.storage_service.redistribute(
            destination_server=request.destination_server,
            lower_val=request.lower_val,
            upper_val=request.upper_val
        )
        self.kv_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.kv_lock.acquire()
        self.storage_service.transfer(
            keys_values=list(request.keys_values)
        )
        self.kv_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.kv_lock.acquire()
        self.storage_service.add_replica(
            server=request.server
        )
        self.kv_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.kv_lock.acquire()
        self.storage_service.remove_replica(
            server=request.server
        )
        self.kv_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()
