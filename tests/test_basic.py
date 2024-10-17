import time
import pytest
from util.waiters import *
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker
import logging
import os

class TestBloomBasic(ValkeyBloomTestCaseBase):

    def test_basic(self):
        client = self.server.get_new_client()
        # Validate that the valkey-bloom module is loaded.
        module_list_data = client.execute_command('MODULE LIST')
        module_list_count = len(module_list_data)
        assert module_list_count == 1
        module_loaded = False
        for module in module_list_data:
            if (module[b'name'] == b'bf'):
                module_loaded = True
                break
        assert(module_loaded)
        # Validate that all the BF.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        bf_cmds = ["BF.ADD", "BF.EXISTS", "BF.MADD", "BF.MEXISTS", "BF.INFO", "BF.CARD", "BF.RESERVE", "BF.INSERT"]
        assert all(item in command_cmd_result for item in bf_cmds)
        # Basic bloom filter create, item add and item exists validation.
        bf_add_result = client.execute_command('BF.ADD filter1 item1')
        assert bf_add_result == 1
        bf_exists_result = client.execute_command('BF.EXISTS filter1 item1')
        assert bf_exists_result == 1
        bf_exists_result = client.execute_command('BF.EXISTS filter1 item2')
        assert bf_exists_result == 0

    def test_copy_and_exists_cmd(self):
        client = self.server.get_new_client()
        madd_result = client.execute_command('BF.MADD filter item1 item2 item3 item4')
        assert client.execute_command('EXISTS filter') == 1
        mexists_result = client.execute_command('BF.MEXISTS filter item1 item2 item3 item4')
        assert len(madd_result) == 4 and len(mexists_result) == 4
        assert client.execute_command('COPY filter new_filter') == 1
        assert client.execute_command('EXISTS new_filter') == 1
        copy_mexists_result = client.execute_command('BF.MEXISTS new_filter item1 item2 item3 item4')
        assert mexists_result == copy_mexists_result
    
    def test_memory_usage_cmd(self):
        client = self.server.get_new_client()
        assert client.execute_command('BF.ADD filter item1') == 1
        memory_usage = client.execute_command('MEMORY USAGE filter')
        info_size = client.execute_command('BF.INFO filter SIZE')
        assert memory_usage > info_size and info_size > 0

    def test_large_allocation_when_below_maxmemory(self):
        two_megabytes = 2 * 1024 * 1024
        # The command below will result in an allocation greater than 2 MB.
        bloom_cmd_large_allocation = 'BF.RESERVE newfilter 0.001 10000000'
        client = self.server.get_new_client()
        assert client.execute_command("CONFIG SET maxmemory-policy allkeys-lru") == b"OK"
        assert client.execute_command("CONFIG SET maxmemory {}".format(two_megabytes)) == b"OK"
        used_memory = client.info_obj().used_memory()
        maxmemory = client.info_obj().maxmemory()
        client.execute_command('BF.ADD filter item1')
        new_used_memory = client.info_obj().used_memory()
        assert new_used_memory > used_memory and new_used_memory < maxmemory
        assert client.execute_command(bloom_cmd_large_allocation) == b"OK"
        assert client.execute_command('DBSIZE') < 2
        assert client.info("Stats")['evicted_keys'] > 0
        used_memory = client.info_obj().used_memory()
        assert used_memory < maxmemory
        client.execute_command('FLUSHALL')
        client.execute_command('BF.ADD filter item1')
        assert client.execute_command("CONFIG SET maxmemory-policy volatile-lru") == b"OK"
        assert client.execute_command(bloom_cmd_large_allocation) == b"OK"
        assert client.execute_command('DBSIZE') == 2
        used_memory = client.info_obj().used_memory()
        assert used_memory > maxmemory

    def test_large_allocation_when_above_maxmemory(self):
        client = self.server.get_new_client()
        assert client.execute_command("CONFIG SET maxmemory-policy allkeys-lru") == b"OK"
        used_memory = client.info_obj().used_memory()
        client.execute_command('BF.ADD filter item1')
        new_used_memory = client.info_obj().used_memory()
        assert new_used_memory > used_memory
        # Configure the server to now be over maxmemory with allkeys-lru policy. Test that allocation fails.
        assert client.execute_command("CONFIG SET maxmemory {}".format(used_memory)) == b"OK"
        bloom_cmd_large_allocation = 'BF.RESERVE newfilter 0.001 10000000'
        self.verify_error_response(self.client, bloom_cmd_large_allocation, "command not allowed when used memory > 'maxmemory'.")
        assert client.info("Errorstats")['errorstat_OOM']['count'] == 1
        # Configure the server to now be over maxmemory with volatile-lru policy. Test that allocation fails.
        assert client.execute_command("CONFIG SET maxmemory-policy volatile-lru") == b"OK"
        self.verify_error_response(self.client, bloom_cmd_large_allocation, "command not allowed when used memory > 'maxmemory'.")
        assert client.info("Errorstats")['errorstat_OOM']['count'] == 2

    def test_module_data_type(self):
        # Validate the name of the Module data type.
        client = self.server.get_new_client()
        assert client.execute_command('BF.ADD filter item1') == 1
        type_result = client.execute_command('TYPE filter')
        assert type_result == b"bloomfltr"
        # Validate the name of the Module data type.
        encoding_result = client.execute_command('OBJECT ENCODING filter')
        assert encoding_result == b"raw"

    def test_bloom_obj_access(self):
        client = self.server.get_new_client()
        # check bloom filter with basic valkey command
        # cmd touch
        assert client.execute_command('BF.ADD key1 val1') == 1
        assert client.execute_command('BF.ADD key2 val2') == 1
        assert client.execute_command('TOUCH key1 key2') == 2
        assert client.execute_command('TOUCH key3') == 0
        self.verify_server_key_count(client, 2)
        assert client.execute_command('DBSIZE') == 2
        random_key = client.execute_command('RANDOMKEY')
        assert random_key == b"key1" or random_key == b"key2"

    def test_bloom_transaction(self):
        client = self.server.get_new_client()
        # cmd multi, exec
        assert client.execute_command('MULTI') == b'OK'
        assert client.execute_command('BF.ADD M1 V1') == b'QUEUED'
        assert client.execute_command('BF.ADD M2 V2') == b'QUEUED'
        assert client.execute_command('BF.EXISTS M1 V1') == b'QUEUED'
        assert client.execute_command('DEL M1') == b'QUEUED'
        assert client.execute_command('BF.EXISTS M1 V1') == b'QUEUED'
        assert client.execute_command('EXEC') == [1, 1, 1, 1, 0]
        self.verify_bloom_filter_item_existence(client, 'M2', 'V2')
        self.verify_bloom_filter_item_existence(client, 'M1', 'V1', should_exist=False)
        self.verify_server_key_count(client, 1)

    def test_bloom_lua(self):
        client = self.server.get_new_client()
        # lua
        load_filter = """
        redis.call('BF.ADD', 'LUA1', 'ITEM1');
        redis.call('BF.ADD', 'LUA2', 'ITEM2');
        redis.call('BF.MADD', 'LUA2', 'ITEM3', 'ITEM4', 'ITEM5');
        """
        client.eval(load_filter, 0)
        assert client.execute_command('BF.MEXISTS LUA2 ITEM1 ITEM3 ITEM4') == [0, 1, 1]
        self.verify_server_key_count(client, 2)

    def test_bloom_deletes(self):
        client = self.server.get_new_client()
        # delete
        assert client.execute_command('BF.ADD filter1 item1') == 1
        self.verify_bloom_filter_item_existence(client, 'filter1', 'item1')
        self.verify_server_key_count(client, 1)
        assert client.execute_command('DEL filter1') == 1
        self.verify_bloom_filter_item_existence(client, 'filter1', 'item1', should_exist=False)
        self.verify_server_key_count(client, 0)

        # flush
        self.create_bloom_filters_and_add_items(client, number_of_bf=10)
        self.verify_server_key_count(client, 10)
        assert client.execute_command('FLUSHALL')
        self.verify_server_key_count(client, 0)

        # unlink
        assert client.execute_command('BF.ADD A ITEMA') == 1
        assert client.execute_command('BF.ADD B ITEMB') == 1
        self.verify_bloom_filter_item_existence(client, 'A', 'ITEMA')
        self.verify_bloom_filter_item_existence(client, 'B', 'ITEMB')
        self.verify_bloom_filter_item_existence(client, 'C', 'ITEMC', should_exist=False)
        self.verify_server_key_count(client, 2)
        assert client.execute_command('UNLINK A B C') == 2
        assert client.execute_command('BF.MEXISTS A ITEMA ITEMB') == [0, 0]
        self.verify_bloom_filter_item_existence(client, 'A', 'ITEMA', should_exist=False)
        self.verify_bloom_filter_item_existence(client, 'B', 'ITEMB', should_exist=False)
        self.verify_server_key_count(client, 0)

    def test_bloom_expiration(self):
        client = self.server.get_new_client()
        # expiration
        # cmd object idletime
        self.verify_server_key_count(client, 0)
        assert client.execute_command('BF.ADD TEST_IDLE val3') == 1
        self.verify_bloom_filter_item_existence(client, 'TEST_IDLE', 'val3')
        self.verify_server_key_count(client, 1)
        time.sleep(1)
        assert client.execute_command('OBJECT IDLETIME test_idle') == None
        assert client.execute_command('OBJECT IDLETIME TEST_IDLE') > 0
        # cmd ttl, expireat
        assert client.execute_command('BF.ADD TEST_EXP ITEM') == 1
        assert client.execute_command('TTL TEST_EXP') == -1
        self.verify_bloom_filter_item_existence(client, 'TEST_EXP', 'ITEM')
        self.verify_server_key_count(client, 2)
        curr_time = int(time.time())
        assert client.execute_command(f'EXPIREAT TEST_EXP {curr_time + 5}') == 1
        wait_for_equal(lambda: client.execute_command('BF.EXISTS TEST_EXP ITEM'), 0)
        self.verify_server_key_count(client, 1)
        # cmd persist
        assert client.execute_command('BF.ADD TEST_PERSIST ITEM') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1
        self.verify_bloom_filter_item_existence(client, 'TEST_PERSIST', 'ITEM')
        self.verify_server_key_count(client, 2)
        assert client.execute_command(f'EXPIREAT TEST_PERSIST {curr_time + 100000}') == 1
        assert client.execute_command('TTL TEST_PERSIST') > 0
        assert client.execute_command('PERSIST TEST_PERSIST') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1
