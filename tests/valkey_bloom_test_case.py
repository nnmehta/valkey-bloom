import os
import pytest
from valkeytests.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import random
import string

class ValkeyBloomTestCaseBase(ValkeyTestCase):

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
        }

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg

    def verify_command_success_reply(self, client, cmd, expected_result):
        try:
            cmd_actual_result = client.execute_command(cmd)
            assert_error_msg = f"Actual command response '{cmd_actual_result}' is different from expected response '{expected_result}'"
            assert cmd_actual_result == expected_result, assert_error_msg
        except:
            print("Something went wrong in command behavior verification")

    def verify_bloom_filter_item_existence(self, client, key, value, should_exist=True):
        try:
            if should_exist:
                assert client.execute_command(f'BF.EXISTS {key} {value}') == 1, f"Item {key} {value} doesn't exist"
            else:
                assert client.execute_command(f'BF.EXISTS {key} {value}') == 0, f"Item {key} {value} exists"
        except:
            print("Something went wrong in bloom filter item existence verification")

    def verify_server_key_count(self, client, expected_num_keys):
        try:
            actual_num_keys = client.num_keys()
            assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_key}"
            assert actual_num_keys == expected_num_keys, assert_num_key_error_msg
        except:
            print("Something went wrong in key number verification")

    def create_bloom_filters_and_add_items(self, client, number_of_bf=5):
        """ Creates the specified number of bloom filter objects (`number_of_bf`) and adds an item to it named FOO.
        """
        for i in range(number_of_bf):
            assert client.execute_command(f'BF.ADD SAMPLE{i} FOO') == 1, f"Failed to insert bloom filter item SAMPLE{i} FOO"

    def generate_random_string(self, length=7):
        """ Creates a random string with specified length.
        """
        characters = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string

    def add_items_till_capacity(self, client, filter_name, capacity_needed, starting_item_idx, rand_prefix, batch_size=1000):
        """
        Adds items to the provided bloom filter object (filter_name) until the specified capacity is reached.
        Item names will start with the provided prefix (rand_prefix) followed by a counter (starting_item_idx onwards).
        """
        new_item_idx = starting_item_idx
        fp_count = 0
        cardinality = client.execute_command(f'BF.CARD {filter_name}')
        while cardinality < capacity_needed:
            # Calculate how many more items we need to add.
            remaining_capacity = capacity_needed - cardinality
            batch_to_add = min(batch_size, remaining_capacity)
            # Prepare a batch of items
            items = [f"{rand_prefix}{new_item_idx + i}" for i in range(batch_to_add)]
            new_item_idx += batch_to_add
            result = client.execute_command(f'BF.MADD {filter_name} ' + ' '.join(items))
            # Process results
            for res in result:
                if res == 0:
                    fp_count += 1
                elif res == 1:
                    cardinality += 1
                else:
                    raise RuntimeError(f"Unexpected return value from add_item: {res}")
        return fp_count, new_item_idx - 1

    def check_items_exist(self, client, filter_name, start_idx, end_idx, expected_result, rand_prefix, batch_size=1000):
        """
        Executes BF.MEXISTS on the given bloom filter. Items that we expect to exist are those starting with
        rand_prefix, followed by a number beginning with start_idx. The result is compared with `expected_result` based
        on whether we expect the item to exist or not.
        """
        error_count = 0
        num_operations = (end_idx - start_idx) + 1
        # Check that items exist in batches.
        for batch_start in range(start_idx, end_idx + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_idx)
            # Execute BF.MEXISTS with the batch of items
            items = [f"{rand_prefix}{i}" for i in range(batch_start, batch_end + 1)]
            result = client.execute_command(f'BF.MEXISTS {filter_name} ' + ' '.join(items))
            # Check the results
            for item_result in result:
                if item_result != expected_result:
                    error_count += 1
        return error_count, num_operations

    def fp_assert(self, error_count, num_operations, expected_fp_rate, fp_margin):
        """
        Asserts that the actual false positive error rate is lower than the expected false positive rate with
        accounting for margin.
        """
        real_fp_rate = error_count / num_operations
        fp_rate_with_margin = expected_fp_rate + fp_margin
        assert real_fp_rate < fp_rate_with_margin, f"The actual fp_rate, {real_fp_rate}, is greater than the configured fp_rate with margin. {fp_rate_with_margin}."

    def validate_copied_bloom_correctness(self, client, original_filter_name, item_prefix, add_operation_idx, expected_fp_rate, fp_margin, original_info_dict):
        """ Validate correctness on a copy of the provided bloom filter.
        """
        copy_filter_name = "filter_copy"
        assert client.execute_command(f'COPY {original_filter_name} {copy_filter_name}') == 1
        assert client.execute_command('DBSIZE') == 2
        copy_info = client.execute_command(f'BF.INFO {copy_filter_name}')
        copy_it = iter(copy_info)
        copy_info_dict = dict(zip(copy_it, copy_it))
        assert copy_info_dict[b'Capacity'] == original_info_dict[b'Capacity']
        assert copy_info_dict[b'Number of items inserted'] == original_info_dict[b'Number of items inserted']
        assert copy_info_dict[b'Number of filters'] == original_info_dict[b'Number of filters']
        assert copy_info_dict[b'Size'] == original_info_dict[b'Size']
        assert copy_info_dict[b'Expansion rate'] == original_info_dict[b'Expansion rate']
        # Items added to the original filter should still exist on the copy. False Negatives are not possible.
        error_count, num_operations = self.check_items_exist(
            client,
            copy_filter_name,
            1,
            add_operation_idx,
            True,
            item_prefix,
        )
        assert error_count == 0
        # Items not added to the original filter should not exist on the copy. False Positives should be close to configured fp_rate.
        error_count, num_operations = self.check_items_exist(
            client,
            copy_filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        self.fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)
