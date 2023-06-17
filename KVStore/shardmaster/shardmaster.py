import logging
import threading
import random

import grpc

from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *

logger = logging.getLogger(__name__)


class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.range_servers = {}

    def join(self, server: str):

        if not self.range_servers:
            self.range_servers[server] = (KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD)
            return

        self.range_servers[server] = (None, None)
        num_servers = len(self.range_servers)
        name_servers = list(self.range_servers.keys())

        for i, server in enumerate(name_servers):
            start = int((i / num_servers) * KEYS_UPPER_THRESHOLD)
            end = int(((i + 1) / num_servers) * KEYS_UPPER_THRESHOLD)
            nextTh = int(((i + 2) / num_servers) * KEYS_UPPER_THRESHOLD)
            self.range_servers[server] = (start, end)

            if i != num_servers - 1:
                next_server = name_servers[i + 1]
                KVStoreStub(grpc.insecure_channel(server)).Redistribute(
                    RedistributeRequest(
                        destination_server=next_server,
                        lower_val=end,
                        upper_val=nextTh
                    )
                )

    def leave(self, server: str):
        if server not in self.range_servers or not self.range_servers:
            return

        del self.range_servers[server]
        num_servers = len(self.range_servers)
        name_servers = list(self.range_servers.keys())

        for i, server in enumerate(name_servers):
            start = int((i / num_servers) * KEYS_UPPER_THRESHOLD)
            end = int(((i + 1) / num_servers) * KEYS_UPPER_THRESHOLD)
            nextTh = int(((i + 2) / num_servers) * KEYS_UPPER_THRESHOLD)
            self.range_servers[server] = (start, end)

            if i != num_servers - 1:
                next_server = name_servers[i + 1]
                KVStoreStub(grpc.insecure_channel(server)).Redistribute(
                    RedistributeRequest(
                        destination_server=next_server,
                        lower_val=end,
                        upper_val=nextTh
                    )
                )

    def query(self, key: int) -> str:
        for server, minMax in self.range_servers.items():
            if minMax[0] <= key <= minMax[1]:
                return server


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        self.number_of_shards = number_of_shards
        self.number_of_replicas = 0
        self.replica_map = {}

    def leave(self, server: str):
        if server in self.range_servers:
            super().leave(server)

        else:
            for server, replica_arr in self.replica_map.items():
                if server in replica_arr:
                    replica_arr.remove(server)
                    stub = KVStoreStub(grpc.insecure_channel(server))
                    stub.RemoveReplica(
                        ServerRequest(
                            server=server))
        self.number_of_replicas -= 1

    def join_replica(self, server: str) -> Role:
        if len(self.range_servers) < self.number_of_shards:
            super().join(server)
            self.replica_map[server] = []
            self.replica_map[server].append(server)
            return Role.Value("MASTER")

        mod = self.number_of_replicas % self.number_of_shards
        master = list(self.replica_map.keys())[mod]
        self.replica_map[master].append(server)
        self.number_of_replicas += 1

        master_stub = KVStoreStub(
            grpc.insecure_channel(master)
        )

        master_stub.AddReplica(
            ServerRequest(
                server=server
            )
        )

        return Role.Value("REPLICA")

    def query_replica(self, key: int, op: Operation) -> str:
        server = super().query(key)
        if op == 0 or op == 3 or op == 4:
            server_replica = random.choice(self.replica_map[server]) or server
            return server_replica
        return server


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
        self.shard_lock = threading.Lock()

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_lock.acquire()
        self.shard_master_service.join(
            server=request.server
        )
        self.shard_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_lock.acquire()
        response = LeaveRequest(
            server=self.shard_master_service.leave(request.server)
        )
        self.shard_lock.release()
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        self.shard_lock.acquire()
        response = QueryResponse(
            server=self.shard_master_service.query(request.key)
        )
        self.shard_lock.release()
        return response

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        self.shard_lock.acquire()
        response = JoinReplicaResponse(
            role=self.shard_master_service.join_replica(request.server)
        )
        self.shard_lock.release()
        return response

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        self.shard_lock.acquire()
        response = QueryResponse(
            server=self.shard_master_service.query_replica(request.key,
                                                           request.operation)
        )
        self.shard_lock.release()
        return response
