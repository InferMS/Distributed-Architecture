import logging
import threading

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

        self.range_servers[server] = ""
        num_servers = len(self.range_servers)
        name_servers = list(self.range_servers.keys())

        for i in range(num_servers):
            start = int((i / num_servers) * KEYS_UPPER_THRESHOLD)
            end = int(((i + 1) / num_servers) * KEYS_UPPER_THRESHOLD)
            self.range_servers[name_servers[i]] = (start, end)

            if i < range(num_servers):
                next_server = name_servers[i + 1]
                KVStoreStub(grpc.insecure_channel(name_servers[i])).Redistribute(
                    RedistributeRequest(destination_server=next_server,
                                        lower_val=end,
                                        upper_val=self.range_servers[next_server][0]))

    def leave(self, server: str):

        if server not in self.range_servers or not self.range_servers:
            return

        del self.range_servers[server]
        num_servers = len(self.range_servers)
        name_servers = list(self.range_servers.keys())

        for i in range(num_servers):
            start = int((i / num_servers) * KEYS_UPPER_THRESHOLD)
            end = int(((i + 1) / num_servers) * KEYS_UPPER_THRESHOLD)
            self.range_servers[name_servers[i]] = (start, end)

            if i < num_servers - 1:
                next_server = name_servers[i + 1]
                KVStoreStub(grpc.insecure_channel(name_servers[i])).Redistribute(
                    RedistributeRequest(destination_server=next_server,
                                        lower_val=end,
                                        upper_val=self.range_servers[next_server][0]))

    def query(self, key: int) -> str:
        num_servers = len(self.range_servers)
        name_servers = list(self.range_servers.keys())
        for i in range(num_servers):
            if self.range_servers[name_servers[i]][0] < key < self.range_servers[name_servers[i]][0]:
                return name_servers[i]


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
        self.shard_lock = threading.Lock()

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_lock.acquire()
        response = JoinRequest(
            server=self.shard_master_service.join(request.server)
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
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
