use crate::configs::{FIXED_SEED, MAX_FILTERS_PER_OBJ, TIGHTENING_RATIO};
use bloomfilter;

/// KeySpace Notification Events
pub const ADD_EVENT: &str = "bloom.add";
pub const RESERVE_EVENT: &str = "bloom.reserve";

/// Errors
pub const ERROR: &str = "ERROR";
pub const NON_SCALING_FILTER_FULL: &str = "ERR non scaling filter is full";
pub const NOT_FOUND: &str = "ERR not found";
pub const ITEM_EXISTS: &str = "ERR item exists";
pub const INVALID_INFO_VALUE: &str = "ERR invalid information value";
pub const BAD_EXPANSION: &str = "ERR bad expansion";
pub const BAD_CAPACITY: &str = "ERR bad capacity";
pub const BAD_ERROR_RATE: &str = "ERR bad error rate";
pub const ERROR_RATE_RANGE: &str = "ERR (0 < error rate range < 1)";
pub const CAPACITY_LARGER_THAN_0: &str = "ERR (capacity should be larger than 0)";
pub const MAX_NUM_SCALING_FILTERS: &str = "ERR max number of scaling filters reached";
pub const UNKNOWN_ARGUMENT: &str = "ERR unknown argument received";

#[derive(Debug, PartialEq)]
pub enum BloomError {
    NonScalingFilterFull,
    MaxNumScalingFilters,
}

impl BloomError {
    pub fn as_str(&self) -> &'static str {
        match self {
            BloomError::NonScalingFilterFull => NON_SCALING_FILTER_FULL,
            BloomError::MaxNumScalingFilters => MAX_NUM_SCALING_FILTERS,
        }
    }
}

/// The BloomFilterType structure. 32 bytes.
/// Can contain one or more filters.
/// This is a generic top level structure which is not coupled to any bloom crate.
pub struct BloomFilterType {
    pub expansion: u32,
    pub fp_rate: f32,
    pub filters: Vec<BloomFilter>,
}

impl BloomFilterType {
    /// Create a new BloomFilterType object.
    pub fn new_reserved(fp_rate: f32, capacity: u32, expansion: u32) -> BloomFilterType {
        let bloom = BloomFilter::new(fp_rate, capacity);
        let filters = vec![bloom];
        BloomFilterType {
            expansion,
            fp_rate,
            filters,
        }
    }

    /// Create a new BloomFilterType object from an existing one.
    pub fn create_copy_from(from_bf: &BloomFilterType) -> BloomFilterType {
        let mut filters = Vec::new();
        for filter in &from_bf.filters {
            let new_filter = BloomFilter::create_copy_from(filter);
            filters.push(new_filter);
        }
        BloomFilterType {
            expansion: from_bf.expansion,
            fp_rate: from_bf.fp_rate,
            filters,
        }
    }

    /// Return the total memory usage of the BloomFilterType object.
    pub fn memory_usage(&self) -> usize {
        let mut mem: usize = std::mem::size_of::<BloomFilterType>();
        for filter in &self.filters {
            mem += filter.number_of_bytes();
        }
        mem
    }

    /// Returns the Bloom object's free_effort.
    /// We return 1 if there are no filters (BF.RESERVE) or if there is 1 filter.
    /// Else, we return the number of filters as the free_effort.
    /// This is similar to how the core handles aggregated objects.
    pub fn free_effort(&self) -> usize {
        if self.filters.is_empty() {
            return 1;
        }
        self.filters.len()
    }

    /// Check if item exists already.
    pub fn item_exists(&self, item: &[u8]) -> bool {
        self.filters.iter().any(|filter| filter.check(item))
    }

    /// Return a count of number of items added to all sub filters in the BloomFilterType object.
    pub fn cardinality(&self) -> i64 {
        let mut cardinality: i64 = 0;
        for filter in &self.filters {
            cardinality += filter.num_items as i64;
        }
        cardinality
    }

    /// Return a total capacity summed across all sub filters in the BloomFilterType object.
    pub fn capacity(&self) -> i64 {
        let mut capacity: i64 = 0;
        // Check if item exists already.
        for filter in &self.filters {
            capacity += filter.capacity as i64;
        }
        capacity
    }

    /// Add an item to the BloomFilterType object.
    /// If scaling is enabled, this can result in a new sub filter creation.
    pub fn add_item(&mut self, item: &[u8]) -> Result<i64, BloomError> {
        // Check if item exists already.
        if self.item_exists(item) {
            return Ok(0);
        }
        let num_filters = self.filters.len() as i32;
        if let Some(filter) = self.filters.last_mut() {
            if filter.num_items < filter.capacity {
                // Add item.
                filter.set(item);
                filter.num_items += 1;
                return Ok(1);
            }
            // Non Scaling Filters that are filled to capacity cannot handle more inserts.
            if self.expansion == 0 {
                return Err(BloomError::NonScalingFilterFull);
            }
            if num_filters == MAX_FILTERS_PER_OBJ {
                return Err(BloomError::MaxNumScalingFilters);
            }
            // Scale out by adding a new filter with capacity bounded within the u32 range.
            let new_fp_rate = self.fp_rate * TIGHTENING_RATIO.powi(num_filters);
            let new_capacity = match filter.capacity.checked_mul(self.expansion) {
                Some(new_capacity) => new_capacity,
                None => u32::MAX,
            };
            let mut new_filter = BloomFilter::new(new_fp_rate, new_capacity);
            // Add item.
            new_filter.set(item);
            new_filter.num_items += 1;
            self.filters.push(new_filter);
            return Ok(1);
        }
        Ok(0)
    }
}

// Structure representing a single bloom filter. 200 Bytes.
// Using Crate: "bloomfilter"
// The reason for using u32 for num_items and capacity is because
// we have a limit on the memory usage of a `BloomFilter` to be 64MB.
// Based on this, we expect the number of items on the `BloomFilter` to be
// well within the u32::MAX limit.
pub struct BloomFilter {
    pub bloom: bloomfilter::Bloom<[u8]>,
    pub num_items: u32,
    pub capacity: u32,
}

impl BloomFilter {
    /// Instantiate empty BloomFilter object.
    pub fn new(fp_rate: f32, capacity: u32) -> BloomFilter {
        let bloom = bloomfilter::Bloom::new_for_fp_rate_with_seed(
            capacity as usize,
            fp_rate as f64,
            &FIXED_SEED,
        );
        BloomFilter {
            bloom,
            num_items: 0,
            capacity,
        }
    }

    /// Create a new BloomFilter from dumped information (RDB load).
    pub fn from_existing(
        bitmap: &[u8],
        number_of_bits: u64,
        number_of_hash_functions: u32,
        sip_keys: [(u64, u64); 2],
        num_items: u32,
        capacity: u32,
    ) -> BloomFilter {
        let bloom = bloomfilter::Bloom::from_existing(
            bitmap,
            number_of_bits,
            number_of_hash_functions,
            sip_keys,
        );
        BloomFilter {
            bloom,
            num_items,
            capacity,
        }
    }

    pub fn number_of_bytes(&self) -> usize {
        std::mem::size_of::<BloomFilter>() + (self.bloom.number_of_bits() / 8) as usize
    }

    pub fn check(&self, item: &[u8]) -> bool {
        self.bloom.check(item)
    }

    pub fn set(&mut self, item: &[u8]) {
        self.bloom.set(item)
    }

    /// Create a new BloomFilter from an existing BloomFilter object (COPY command).
    pub fn create_copy_from(bf: &BloomFilter) -> BloomFilter {
        BloomFilter::from_existing(
            &bf.bloom.bitmap(),
            bf.bloom.number_of_bits(),
            bf.bloom.number_of_hash_functions(),
            bf.bloom.sip_keys(),
            bf.num_items,
            bf.capacity,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::{
        FIXED_SIP_KEY_ONE_A, FIXED_SIP_KEY_ONE_B, FIXED_SIP_KEY_TWO_A, FIXED_SIP_KEY_TWO_B,
    };
    use rand::{distributions::Alphanumeric, Rng};

    /// Returns random string with specified number of characters.
    fn random_prefix(len: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    /// Loops until the capacity of the provided bloom filter is reached and adds a new item to it in every iteration.
    /// The item name is rand_prefix + the index (starting from starting_item_idx).
    /// With every add operation, fp_count is tracked as we expect the add operation to return 1, since it is a new item.
    /// Returns the number of errors (false positives) and the final item index.
    fn add_items_till_capacity(
        bf: &mut BloomFilterType,
        capacity_needed: i64,
        starting_item_idx: i64,
        rand_prefix: &String,
    ) -> (i64, i64) {
        let mut new_item_idx = starting_item_idx;
        let mut fp_count = 0;
        let mut cardinality = bf.cardinality();
        while cardinality < capacity_needed {
            let item = format!("{}{}", rand_prefix, new_item_idx);
            let result = bf.add_item(item.as_bytes());
            match result {
                Ok(0) => {
                    fp_count += 1;
                }
                Ok(1) => {
                    cardinality += 1;
                }
                Ok(i64::MIN..=-1_i64) | Ok(2_i64..=i64::MAX) => {
                    panic!("We do not expect add_item to return any Integer other than 0 or 1.")
                }
                Err(e) => {
                    panic!("We do not expect add_item to throw errors on this scalable filter test, {:?}", e);
                }
            };
            new_item_idx += 1;
        }
        (fp_count, new_item_idx - 1)
    }

    /// Loops from the start index till the end index and uses the exists operation on the provided bloom filter.
    /// The item name used in exists operations is rand_prefix + the index (based on the iteration).
    /// The results are matched against the `expected_result` and an error_count tracks the wrong results.
    /// Asserts that the error_count is within the expected false positive (+ margin) rate.
    /// Returns the error count and number of operations performed.
    fn check_items_exist(
        bf: &BloomFilterType,
        start_idx: i64,
        end_idx: i64,
        expected_result: bool,
        rand_prefix: &String,
    ) -> (i64, i64) {
        let mut error_count = 0;
        for i in start_idx..=end_idx {
            let item = format!("{}{}", rand_prefix, i);
            let result = bf.item_exists(item.as_bytes());
            if result != expected_result {
                error_count += 1;
            }
        }
        let num_operations = (end_idx - start_idx) + 1;
        (error_count, num_operations)
    }

    fn fp_assert(error_count: i64, num_operations: i64, expected_fp_rate: f32, fp_margin: f32) {
        let real_fp_rate = error_count as f32 / num_operations as f32;
        let fp_rate_with_margin = expected_fp_rate + fp_margin;
        assert!(
            real_fp_rate < fp_rate_with_margin,
            "The actual fp_rate, {}, is greater than the configured fp_rate with margin. {}.",
            real_fp_rate,
            fp_rate_with_margin
        );
    }

    fn verify_restored_items(
        original_bloom_filter_type: &BloomFilterType,
        restored_bloom_filter_type: &BloomFilterType,
        add_operation_idx: i64,
        expected_fp_rate: f32,
        fp_margin: f32,
        rand_prefix: &String,
    ) {
        let expected_sip_keys = [
            (FIXED_SIP_KEY_ONE_A, FIXED_SIP_KEY_ONE_B),
            (FIXED_SIP_KEY_TWO_A, FIXED_SIP_KEY_TWO_B),
        ];
        assert_eq!(
            restored_bloom_filter_type.capacity(),
            original_bloom_filter_type.capacity()
        );
        assert_eq!(
            restored_bloom_filter_type.cardinality(),
            original_bloom_filter_type.cardinality(),
        );
        assert_eq!(
            restored_bloom_filter_type.free_effort(),
            original_bloom_filter_type.free_effort()
        );
        assert_eq!(
            restored_bloom_filter_type.memory_usage(),
            original_bloom_filter_type.memory_usage()
        );
        assert!(restored_bloom_filter_type
            .filters
            .iter()
            .all(|restore_filter| original_bloom_filter_type
                .filters
                .iter()
                .any(
                    |filter| (filter.bloom.sip_keys() == restore_filter.bloom.sip_keys())
                        && (restore_filter.bloom.sip_keys() == expected_sip_keys)
                )));
        assert!(restored_bloom_filter_type
            .filters
            .iter()
            .all(|restore_filter| original_bloom_filter_type
                .filters
                .iter()
                .any(|filter| filter.bloom.number_of_hash_functions()
                    == restore_filter.bloom.number_of_hash_functions())));
        assert!(restored_bloom_filter_type
            .filters
            .iter()
            .all(|restore_filter| original_bloom_filter_type
                .filters
                .iter()
                .any(|filter| filter.bloom.bitmap() == restore_filter.bloom.bitmap())));
        let (error_count, _) = check_items_exist(
            restored_bloom_filter_type,
            1,
            add_operation_idx,
            true,
            rand_prefix,
        );
        assert!(error_count == 0);
        let (error_count, num_operations) = check_items_exist(
            restored_bloom_filter_type,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            rand_prefix,
        );
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);
    }

    #[test]
    fn test_non_scaling_filter() {
        let rand_prefix = random_prefix(7);
        // 1 in every 1000 operations is expected to be a false positive.
        let expected_fp_rate: f32 = 0.001;
        let initial_capacity = 10000;
        // Expansion of 0 indicates non scaling.
        let expansion = 0;
        // Validate the non scaling behavior of the bloom filter.
        let mut bf = BloomFilterType::new_reserved(expected_fp_rate, initial_capacity, expansion);
        let (error_count, add_operation_idx) =
            add_items_till_capacity(&mut bf, initial_capacity as i64, 1, &rand_prefix);
        assert_eq!(
            bf.add_item(b"new_item"),
            Err(BloomError::NonScalingFilterFull)
        );
        assert_eq!(bf.capacity(), initial_capacity as i64);
        assert_eq!(bf.cardinality(), initial_capacity as i64);
        assert_eq!(bf.free_effort(), 1);
        assert!(bf.memory_usage() > 0);
        // Use a margin on the expected_fp_rate when asserting for correctness.
        let fp_margin = 0.002;
        // Validate that item "add" operations on bloom filters are ensuring correctness.
        fp_assert(error_count, add_operation_idx, expected_fp_rate, fp_margin);
        // Validate item "exists" operations on bloom filters are ensuring correctness.
        // This tests for items already added to the filter and expects them to exist.
        let (error_count, _) = check_items_exist(&bf, 1, add_operation_idx, true, &rand_prefix);
        assert!(error_count == 0);
        // This tests for items which are not added to the filter and expects them to not exist.
        let (error_count, num_operations) = check_items_exist(
            &bf,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            &rand_prefix,
        );
        // Validate that the real fp_rate is not much more than the configured fp_rate.
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);

        // Verify restore
        let mut restore_bf = BloomFilterType::create_copy_from(&bf);
        assert_eq!(
            restore_bf.add_item(b"new_item"),
            Err(BloomError::NonScalingFilterFull)
        );
        verify_restored_items(
            &bf,
            &restore_bf,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
            &rand_prefix,
        );
    }

    #[test]
    fn test_scaling_filter() {
        let rand_prefix = random_prefix(7);
        // 1 in every 1000 operations is expected to be a false positive.
        let expected_fp_rate: f32 = 0.001;
        let initial_capacity = 10000;
        let expansion = 2;
        let num_filters_to_scale = 5;
        let mut bf = BloomFilterType::new_reserved(expected_fp_rate, initial_capacity, expansion);
        assert_eq!(bf.capacity(), initial_capacity as i64);
        assert_eq!(bf.cardinality(), 0);
        let mut total_error_count = 0;
        let mut add_operation_idx = 0;
        // Validate the scaling behavior of the bloom filter.
        for filter_idx in 1..=num_filters_to_scale {
            let expected_total_capacity = initial_capacity * (expansion.pow(filter_idx) - 1);
            let (error_count, new_add_operation_idx) = add_items_till_capacity(
                &mut bf,
                expected_total_capacity as i64,
                add_operation_idx + 1,
                &rand_prefix,
            );
            add_operation_idx = new_add_operation_idx;
            total_error_count += error_count;
            assert_eq!(bf.capacity(), expected_total_capacity as i64);
            assert_eq!(bf.cardinality(), expected_total_capacity as i64);
            assert_eq!(bf.free_effort(), filter_idx as usize);
            assert!(bf.memory_usage() > 0);
        }
        // Use a margin on the expected_fp_rate when asserting for correctness.
        let fp_margin = 0.002;
        // Validate that item "add" operations on bloom filters are ensuring correctness.
        fp_assert(
            total_error_count,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
        );
        // Validate item "exists" operations on bloom filters are ensuring correctness.
        // This tests for items already added to the filter and expects them to exist.
        let (error_count, _) = check_items_exist(&bf, 1, add_operation_idx, true, &rand_prefix);
        assert!(error_count == 0);
        // This tests for items which are not added to the filter and expects them to not exist.
        let (error_count, num_operations) = check_items_exist(
            &bf,
            add_operation_idx + 1,
            add_operation_idx * 2,
            false,
            &rand_prefix,
        );
        // Validate that the real fp_rate is not much more than the configured fp_rate.
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin);

        // Verify restore
        let restore_bloom_filter_type = BloomFilterType::create_copy_from(&bf);
        verify_restored_items(
            &bf,
            &restore_bloom_filter_type,
            add_operation_idx,
            expected_fp_rate,
            fp_margin,
            &rand_prefix,
        );
    }

    #[test]
    fn test_sip_keys() {
        // The value of sip keys generated by the sip_keys with fixed seed should be equal to the constant in configs.rs
        let test_bloom_filter = BloomFilter::new(0.5_f32, 1000_u32);
        let test_sip_keys = test_bloom_filter.bloom.sip_keys();
        assert_eq!(test_sip_keys[0].0, FIXED_SIP_KEY_ONE_A);
        assert_eq!(test_sip_keys[0].1, FIXED_SIP_KEY_ONE_B);
        assert_eq!(test_sip_keys[1].0, FIXED_SIP_KEY_TWO_A);
        assert_eq!(test_sip_keys[1].1, FIXED_SIP_KEY_TWO_B);
    }
}
