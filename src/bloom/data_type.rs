use crate::bloom::utils::BloomFilter;
use crate::bloom::utils::BloomFilterType;
use crate::configs::{
    FIXED_SIP_KEY_ONE_A, FIXED_SIP_KEY_ONE_B, FIXED_SIP_KEY_TWO_A, FIXED_SIP_KEY_TWO_B,
};
use crate::wrapper::bloom_callback;
use crate::MODULE_NAME;
use std::os::raw::c_int;
use valkey_module::native_types::ValkeyType;
use valkey_module::{logging, raw};

const BLOOM_FILTER_TYPE_ENCODING_VERSION: i32 = 0;

pub static BLOOM_FILTER_TYPE: ValkeyType = ValkeyType::new(
    "bloomfltr",
    BLOOM_FILTER_TYPE_ENCODING_VERSION,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(bloom_callback::bloom_rdb_load),
        rdb_save: Some(bloom_callback::bloom_rdb_save),
        // TODO
        aof_rewrite: None,

        mem_usage: Some(bloom_callback::bloom_mem_usage),
        // TODO
        digest: None,
        free: Some(bloom_callback::bloom_free),

        aux_load: Some(bloom_callback::bloom_aux_load),
        // Callback not needed as there is no AUX (out of keyspace) data to be saved.
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: raw::Aux::Before as i32,

        free_effort: Some(bloom_callback::bloom_free_effort),
        // Callback not needed as it just notifies us when a bloom item is about to be freed.
        unlink: None,
        copy: Some(bloom_callback::bloom_copy),
        defrag: Some(bloom_callback::bloom_defrag),

        // The callbacks below are not needed since the version 1 variants are used when implemented.
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
    },
);

/// Callback to load and parse RDB data of a bloom item and create it.
pub fn bloom_rdb_load_data_object(
    rdb: *mut raw::RedisModuleIO,
    encver: i32,
) -> Option<BloomFilterType> {
    if encver > BLOOM_FILTER_TYPE_ENCODING_VERSION {
        logging::log_warning(format!("{}: Cannot load bloomfltr data type of version {} because it is higher than the loaded module's bloomfltr supported version {}", MODULE_NAME, encver, BLOOM_FILTER_TYPE_ENCODING_VERSION).as_str());
        return None;
    }
    let Ok(num_filters) = raw::load_unsigned(rdb) else {
        return None;
    };
    let Ok(expansion) = raw::load_unsigned(rdb) else {
        return None;
    };
    let Ok(fp_rate) = raw::load_float(rdb) else {
        return None;
    };
    let mut filters = Vec::new();
    for i in 0..num_filters {
        let Ok(bitmap) = raw::load_string_buffer(rdb) else {
            return None;
        };
        let Ok(number_of_bits) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(number_of_hash_functions) = raw::load_unsigned(rdb) else {
            return None;
        };
        let Ok(capacity) = raw::load_unsigned(rdb) else {
            return None;
        };
        // Only load num_items when it's the last filter
        let num_items = if i == num_filters - 1 {
            match raw::load_unsigned(rdb) {
                Ok(num_items) => num_items,
                Err(_) => return None,
            }
        } else {
            capacity
        };
        let sip_keys = [
            (FIXED_SIP_KEY_ONE_A, FIXED_SIP_KEY_ONE_B),
            (FIXED_SIP_KEY_TWO_A, FIXED_SIP_KEY_TWO_B),
        ];
        let filter = BloomFilter::from_existing(
            bitmap.as_ref(),
            number_of_bits,
            number_of_hash_functions as u32,
            sip_keys,
            num_items as u32,
            capacity as u32,
        );
        filters.push(filter);
    }
    let item = BloomFilterType {
        expansion: expansion as u32,
        fp_rate,
        filters,
    };
    Some(item)
}

/// Load the auxiliary data outside of the regular keyspace from the RDB file
pub fn bloom_rdb_aux_load(_rdb: *mut raw::RedisModuleIO) -> c_int {
    logging::log_notice("Ignoring AUX fields during RDB load.");
    raw::Status::Ok as i32
}
