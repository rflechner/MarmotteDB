use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir {
    root: PathBuf,
    path: PathBuf,
}

impl TestDir {
    fn new() -> Self {
        let root = std::env::temp_dir().canonicalize().unwrap();
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = root.join(format!(
            "marmotte-index-{}-{stamp}-{id}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self { root, path }
    }
    fn folder(&self) -> String {
        self.path.to_str().unwrap().to_owned()
    }
    fn strings(&self, capacity: u32, shift: u32, limit: u32) -> SortedIndexFiles<String> {
        SortedIndexFiles::new(
            self.folder(),
            String::new(),
            default_string_reader(),
            default_string_writer(),
            limit,
            shift,
            capacity,
        )
        .unwrap()
    }
    fn integers(&self, capacity: u32, shift: u32, limit: u32) -> SortedIndexFiles<u32> {
        SortedIndexFiles::new(
            self.folder(),
            0,
            default_u32_reader(),
            default_u32_writer(),
            limit,
            shift,
            capacity,
        )
        .unwrap()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        // Only remove the uniquely created directory directly inside the OS temp root.
        if let Ok(resolved) = self.path.canonicalize() {
            assert_eq!(resolved.parent(), Some(self.root.as_path()));
            assert!(
                resolved
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("marmotte-index-")
            );
            let _ = fs::remove_dir_all(resolved);
        }
    }
}

fn pairs<T: Ord + BinarySizeable>(items: Vec<FenseIndex<T>>) -> Vec<(T, u64)> {
    items.into_iter().map(|ix| (ix.value, ix.target)).collect()
}

fn assert_fragments<T: Ord + Clone + BinarySizeable + std::fmt::Debug>(
    files: &mut SortedIndexFiles<T>,
) {
    for num in 0..files.fragment_count() {
        let header = files.read_header(num).unwrap();
        let entries = files.read_fragment(num).unwrap();
        assert_eq!(header.records_count as usize, entries.len());
        assert!(header.records_count <= header.max_records_count);
        assert!(entries.windows(2).all(|w| compare(&w[0], &w[1]).is_le()));
        if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
            assert_eq!(header.min_value, first.value);
            assert_eq!(header.max_value, last.value);
        }
    }
}

#[test]
fn should_create_new_file_when_is_more_records_than_max_records_count_per_fragment() {
    let dir = TestDir::new();
    let mut files = dir.strings(20, 5, 3);
    let mut expected = Vec::new();
    for i in 0..65 {
        let value = format!("string value {i}");
        files
            .store(FenseIndex::from_value(i, value.clone()), SLOT_SIZE)
            .unwrap();
        expected.push((value, i));
    }
    expected.sort();
    // File numbers are creation order, not global key order; do not assume four files.
    assert!(files.fragment_count() >= 4);
    assert_fragments(&mut files);
    assert_eq!(pairs(files.all().unwrap()), expected);
    files.flush().unwrap();
    drop(files);
    let mut reopened = dir.strings(20, 5, 3);
    assert_eq!(pairs(reopened.all().unwrap()), expected);
}

#[test]
fn should_find_index_file_num_for_index_value() {
    let dir = TestDir::new();
    let mut files = dir.strings(1000, 10, 3);
    for num in 0..10 {
        files.open_fragment(num).unwrap();
        let letter = (b'a' + num as u8) as char;
        for i in num..num + 20 {
            let value = format!("string value {letter} - {}", i * 10);
            files
                .write_offset(num, FenseIndex::from_value(100 * i as u64, value), i as u32)
                .unwrap();
        }
    }
    let mut table = SortedIndexTableFragment::new(&mut files);
    for value in ["string value d - 15", "string value g - 20"] {
        let assignment = table
            .get_index_file_num_for_store(&FenseIndex::from_value(100, value.to_owned()))
            .unwrap();
        assert_eq!(assignment, FileNumberAssignment::Specific(0));
    }
}

#[test]
fn should_read_offset_by_offset() {
    let dir = TestDir::new();
    let mut files = dir.strings(500, 10, 3);
    files.open_fragment(0).unwrap();
    for i in 0..500 {
        files
            .write_offset(
                0,
                FenseIndex::from_value(100 * i as u64, format!("string value {i}")),
                i,
            )
            .unwrap();
    }
    let header = files.read_header(0).unwrap();
    let items = files
        .read_all_indexes(0, SLOT_SIZE, header.compute_binary_size() as u64)
        .unwrap();
    for (i, item) in items.iter().enumerate() {
        assert_eq!(item.value, format!("string value {i}"));
        assert_eq!(item.target, 100 * i as u64);
    }
    assert_eq!(items.len(), 500);
}

#[test]
fn should_read_only_written_index_records() {
    let dir = TestDir::new();
    let mut files = dir.strings(500, 10, 3);
    files.open_fragment(0).unwrap();
    for i in 20..30 {
        let value = pad_or_truncate_string(format!("string value {i}"), ' ', 200);
        files
            .write_offset(0, FenseIndex::from_value(100 * i as u64, value), i)
            .unwrap();
    }
    let items = files.read_fragment(0).unwrap();
    assert_eq!(items.len(), 10);
    for (item, i) in items.iter().zip(20..30) {
        assert_eq!(item.value.trim(), format!("string value {i}"));
    }
    drop(files);
    let mut reopened = dir.strings(500, 10, 3);
    assert!(reopened.read_offset(0, 19).unwrap().is_none());
    assert_eq!(reopened.read_fragment(0).unwrap(), items);
}

#[test]
fn should_read_only_written_u32_index_records() {
    let dir = TestDir::new();
    let mut files = dir.integers(500, 10, 3);
    files.open_fragment(0).unwrap();
    for i in 20..30 {
        let item = FenseIndex::from_value(100 * i as u64, i);
        files.write_offset(0, item, i).unwrap();
        files.write_offset(0, item, i).unwrap();
    }
    assert_eq!(files.read_header(0).unwrap().records_count, 10);
    assert_eq!(
        pairs(files.read_fragment(0).unwrap()),
        (20..30)
            .map(|i| (i, u64::from(i) * 100))
            .collect::<Vec<_>>()
    );
}

#[test]
fn should_write_and_read_index_file_header() {
    let dir = TestDir::new();
    let mut files = dir.strings(50, 10, 3);
    files.open_fragment(0).unwrap();
    let header = files.read_header(0).unwrap();
    assert_eq!(header.records_count, 0);
    assert_eq!(header.max_records_count, 50);
    assert_eq!(header.shift_threshold, 10);
    assert_eq!(header.min_value, "");
    assert_eq!(header.max_value, "");
    assert_eq!(header.compute_binary_size(), HEADER_SIZE);
}

#[test]
fn string_index_should_be_greater() {
    assert!(
        compare(
            &FenseIndex::from_value(2, "bbbb".to_owned()),
            &FenseIndex::from_value(1, "aaaa".to_owned())
        )
        .is_gt()
    );
}

#[test]
fn string_index_should_be_less() {
    assert!(
        compare(
            &FenseIndex::from_value(2, "bbbb".to_owned()),
            &FenseIndex::from_value(1, "zzzz".to_owned())
        )
        .is_lt()
    );
}

#[test]
fn string_index_should_be_equal() {
    assert_eq!(
        FenseIndex::from_value(1, "ddd".to_owned()).value,
        FenseIndex::from_value(2, "ddd".to_owned()).value
    );
}

#[test]
fn u64_index_should_be_greater() {
    assert!(
        compare(
            &FenseIndex::from_value(2, 60u64),
            &FenseIndex::from_value(1, 45u64)
        )
        .is_gt()
    );
}

#[test]
fn split_preserves_the_original_value_pivot_and_targets() {
    let dir = TestDir::new();
    let mut files = dir.integers(4, 4, 10);
    for value in [10, 20, 30, 40] {
        files
            .insert(FenseIndex::from_value(value as u64 * 10, value))
            .unwrap();
    }
    let item = FenseIndex::from_value(250, 25);
    assert_eq!(
        files.assignment(&item).unwrap(),
        FileNumberAssignment::Split(0)
    );
    SortedIndexTableFragment::new(&mut files)
        .insert(item)
        .unwrap();
    assert_eq!(
        pairs(files.read_fragment(0).unwrap()),
        vec![(10, 100), (20, 200), (25, 250)]
    );
    assert_eq!(
        pairs(files.read_fragment(1).unwrap()),
        vec![(30, 300), (40, 400)]
    );
    assert_fragments(&mut files);
    drop(files);
    let mut reopened = dir.integers(4, 4, 10);
    assert_fragments(&mut reopened);
    assert_eq!(reopened.find(&25).unwrap(), vec![item]);
}

#[test]
fn threshold_counts_shifted_slots_and_allows_exactly_the_limit() {
    let dir = TestDir::new();
    let mut files = dir.integers(10, 1, 10);
    for value in [20, 40, 30] {
        files
            .insert(FenseIndex::from_value(value as u64, value))
            .unwrap();
    }
    assert_eq!(files.fragment_count(), 1); // 30 shifted only 40.
    files.insert(FenseIndex::from_value(10, 10)).unwrap();
    assert_eq!(files.fragment_count(), 2); // Three shifts would exceed one.
    assert_eq!(
        pairs(files.all().unwrap()),
        vec![(10, 10), (20, 20), (30, 30), (40, 40)]
    );
}

#[test]
fn incomplete_fragments_are_automatically_compacted_and_reopened() {
    let dir = TestDir::new();
    let mut files = dir.integers(4, 0, 2);
    for value in [40, 30, 20] {
        files
            .insert(FenseIndex::from_value(value as u64, value))
            .unwrap();
    }
    assert_eq!(files.fragment_count(), 1);
    assert_eq!(
        SortedIndexFiles::<u32>::count_fragments_in_folder(dir.folder()).unwrap(),
        1
    );
    assert_eq!(
        pairs(files.all().unwrap()),
        vec![(20, 20), (30, 30), (40, 40)]
    );
    drop(files);
    let mut reopened = dir.integers(4, 0, 2);
    reopened.insert(FenseIndex::from_value(10, 10)).unwrap();
    assert_eq!(
        pairs(reopened.all().unwrap()),
        vec![(10, 10), (20, 20), (30, 30), (40, 40)]
    );
}

#[test]
fn empty_zero_unicode_and_variable_lengths_survive_replacement_sorting_and_reopen() {
    let dir = TestDir::new();
    let mut files = dir.strings(8, 8, 3);
    files.open_fragment(0).unwrap();
    for (offset, value) in ["", "é", "🦫", "a long string"].iter().enumerate() {
        files
            .write_offset(
                0,
                FenseIndex::from_value(offset as u64, value.to_string()),
                offset as u32,
            )
            .unwrap();
    }
    files
        .write_offset(0, FenseIndex::from_value(1, "日本語".repeat(100)), 1)
        .unwrap();
    files
        .write_offset(0, FenseIndex::from_value(3, "x".to_owned()), 3)
        .unwrap();
    let expected = vec![
        ("".to_owned(), 0),
        ("x".to_owned(), 3),
        ("日本語".repeat(100), 1),
        ("🦫".to_owned(), 2),
    ];
    let path = files.fragment_path(0);
    let before = fs::read(&path).unwrap();
    files
        .reorder_indexes(0, SLOT_SIZE, HEADER_SIZE as u64)
        .unwrap();
    let after = fs::read(&path).unwrap();
    let payload_start = directory_end(8) as usize;
    assert_eq!(before[payload_start..], after[payload_start..]); // Sorting only rewrites pointers.
    assert_eq!(pairs(files.all().unwrap()), expected);
    assert_eq!(files.read_header(0).unwrap().min_value, "");
    drop(files);
    let mut reopened = dir.strings(8, 8, 3);
    assert_eq!(pairs(reopened.all().unwrap()), expected);
    assert_eq!(
        pairs(reopened.find(&"".to_owned()).unwrap()),
        vec![("".to_owned(), 0)]
    );
}

#[test]
fn clearing_is_idempotent_updates_bounds_and_compaction_reclaims_payloads() {
    let dir = TestDir::new();
    let mut files = dir.strings(8, 8, 3);
    for value in ["a", "b", "z"] {
        files
            .insert(FenseIndex::from_value(0, value.repeat(100)))
            .unwrap();
    }
    let size_before = fs::metadata(files.fragment_path(0)).unwrap().len();
    files.clear_offset(0, 0).unwrap();
    files.clear_offset(0, 0).unwrap();
    assert_eq!(files.read_header(0).unwrap().records_count, 2);
    assert_eq!(files.read_header(0).unwrap().min_value, "b".repeat(100));
    files.clear_offset(0, 2).unwrap();
    assert_eq!(files.read_header(0).unwrap().max_value, "b".repeat(100));
    files.compact().unwrap();
    assert!(fs::metadata(files.fragment_path(0)).unwrap().len() < size_before);
    files.clear_offset(0, 0).unwrap();
    assert_eq!(files.read_header(0).unwrap().records_count, 0);
    files.compact().unwrap();
    assert_eq!(files.fragment_count(), 0);
    drop(files);
    let mut reopened = dir.strings(8, 8, 3);
    reopened
        .insert(FenseIndex::from_value(42, String::new()))
        .unwrap();
    assert_eq!(pairs(reopened.all().unwrap()), vec![(String::new(), 42)]);
}

#[test]
fn duplicate_values_and_targets_at_fragment_boundaries_are_never_lost() {
    for capacity in [1, 2, 4] {
        let dir = TestDir::new();
        let mut files = dir.integers(capacity, capacity, 3);
        let mut expected = Vec::new();
        for (value, target) in [
            (0, 9),
            (0, 2),
            (u32::MAX, 0),
            (0, 2),
            (u32::MAX, 3),
            (7, 4),
            (7, 1),
            (0, 0),
        ] {
            files.insert(FenseIndex::from_value(target, value)).unwrap();
            expected.push((value, target));
        }
        expected.sort();
        assert_fragments(&mut files);
        assert_eq!(pairs(files.all().unwrap()), expected);
        assert_eq!(
            pairs(files.find(&0).unwrap()),
            vec![(0, 0), (0, 2), (0, 2), (0, 9)]
        );
        files.compact().unwrap();
        drop(files);
        let mut reopened = dir.integers(capacity, capacity, 3);
        assert_eq!(pairs(reopened.all().unwrap()), expected);
    }
}

#[test]
fn split_handles_repeated_pivot_values() {
    let dir = TestDir::new();
    let mut files = dir.integers(6, 6, 10);
    for (target, value) in [10, 20, 20, 20, 30, 40].into_iter().enumerate() {
        files
            .insert(FenseIndex::from_value(target as u64, value))
            .unwrap();
    }
    files.insert(FenseIndex::from_value(0, 20)).unwrap();
    assert_eq!(files.read_header(0).unwrap().records_count, 5);
    assert_eq!(
        pairs(files.find(&20).unwrap()),
        vec![(20, 0), (20, 1), (20, 2), (20, 3)]
    );
    assert_fragments(&mut files);
}

#[test]
fn range_search_merges_overlapping_fragments_and_includes_endpoints() {
    let dir = TestDir::new();
    let mut files = dir.integers(5, 5, 10);
    for (num, values) in [[10, 30, 50], [20, 30, 40]].into_iter().enumerate() {
        files.open_fragment(num).unwrap();
        for (offset, value) in values.into_iter().enumerate() {
            files
                .write_offset(
                    num,
                    FenseIndex::from_value((num * 10 + offset) as u64, value),
                    offset as u32,
                )
                .unwrap();
        }
    }
    assert_eq!(
        pairs(files.range(&20, &40).unwrap()),
        vec![(20, 10), (30, 1), (30, 11), (40, 12)]
    );
    assert!(files.range(&60, &70).unwrap().is_empty());
    assert!(files.range(&2, &1).is_err());
}

#[test]
fn fragment_opening_is_idempotent_and_append_returns_its_index() {
    let dir = TestDir::new();
    let mut files = dir.integers(4, 2, 3);
    assert_eq!(files.append_fragment().unwrap(), 0);
    files.open_fragment(0).unwrap();
    assert_eq!(files.fragment_count(), 1);
    assert!(files.open_fragment(2).is_err());
    assert_eq!(files.append_fragment().unwrap(), 1);
    drop(files);
    let mut reopened = dir.integers(4, 2, 3);
    assert_eq!(reopened.fragment_count(), 2);
    reopened.open_fragment(1).unwrap();
    assert_eq!(reopened.append_fragment().unwrap(), 2);
}

#[test]
fn invalid_insertions_and_offsets_do_not_change_existing_entries() {
    let dir = TestDir::new();
    let mut files = dir.integers(2, 2, 3);
    files.insert(FenseIndex::from_value(1, 0)).unwrap();
    let original = fs::read(files.fragment_path(0)).unwrap();
    assert!(files.insert(FenseIndex::new(2, 42, 3)).is_err());
    assert!(
        files
            .insert(FenseIndex {
                active: false,
                ..FenseIndex::from_value(2, 42)
            })
            .is_err()
    );
    assert!(
        files
            .write_offset(0, FenseIndex::from_value(2, 42), 2)
            .is_err()
    );
    assert!(files.read_offset(8, 0).is_err());
    assert!(files.clear_offset(0, 2).is_err());
    assert_eq!(fs::read(files.fragment_path(0)).unwrap(), original);
    assert_eq!(pairs(files.all().unwrap()), vec![(0, 1)]);
}

#[test]
fn malformed_files_and_old_format_are_rejected_without_modifying_them() {
    let original_dir = TestDir::new();
    let mut files = original_dir.strings(4, 2, 3);
    files
        .insert(FenseIndex::from_value(1, "value".to_owned()))
        .unwrap();
    let original = fs::read(files.fragment_path(0)).unwrap();
    drop(files);

    let mut mutations: Vec<Vec<u8>> = vec![
        vec![0; 100],
        original[..10].to_vec(),
        original[..30].to_vec(),
    ];
    let mut wrong_magic = original.clone();
    wrong_magic[0] = 0;
    mutations.push(wrong_magic);
    let mut wrong_count = original.clone();
    wrong_count[12..16].copy_from_slice(&2u32.to_be_bytes());
    mutations.push(wrong_count);
    let mut bad_flag = original.clone();
    bad_flag[HEADER_SIZE] = 9;
    mutations.push(bad_flag);
    let mut bad_offset = original.clone();
    bad_offset[HEADER_SIZE + 9..HEADER_SIZE + 17].copy_from_slice(&1u64.to_be_bytes());
    mutations.push(bad_offset);
    let mut overflow_offset = original.clone();
    overflow_offset[HEADER_SIZE + 9..HEADER_SIZE + 17].copy_from_slice(&u64::MAX.to_be_bytes());
    mutations.push(overflow_offset);
    let mut bad_utf8 = original.clone();
    bad_utf8[directory_end(4) as usize] = 0xff;
    mutations.push(bad_utf8);
    let mut truncated_value = original.clone();
    truncated_value.pop();
    mutations.push(truncated_value);

    for bytes in mutations {
        let dir = TestDir::new();
        let path = dir.path.join("00000000.ix");
        fs::write(&path, &bytes).unwrap();
        let opened = SortedIndexFiles::new(
            dir.folder(),
            String::new(),
            default_string_reader(),
            default_string_writer(),
            3,
            2,
            4,
        );
        assert!(opened.is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn configuration_mismatch_and_missing_fragment_are_reported() {
    let dir = TestDir::new();
    let mut files = dir.integers(4, 2, 3);
    files.insert(FenseIndex::from_value(1, 10)).unwrap();
    drop(files);
    for (capacity, shift) in [(0, 2), (5, 2), (4, 3)] {
        assert!(
            SortedIndexFiles::new(
                dir.folder(),
                0,
                default_u32_reader(),
                default_u32_writer(),
                3,
                shift,
                capacity
            )
            .is_err()
        );
    }
    fs::rename(dir.path.join("00000000.ix"), dir.path.join("00000001.ix")).unwrap();
    assert!(
        SortedIndexFiles::new(
            dir.folder(),
            0,
            default_u32_reader(),
            default_u32_writer(),
            3,
            2,
            4
        )
        .is_err()
    );
}

#[test]
fn encoding_failure_during_compaction_leaves_original_files_intact() {
    use std::cell::Cell;
    use std::rc::Rc;
    let dir = TestDir::new();
    let fail = Rc::new(Cell::new(false));
    let flag = fail.clone();
    let writer: ValueWriter<u32> = Box::new(move |value| {
        if flag.get() && value == 20 {
            return Err("Injected codec failure".into());
        }
        Ok(Bytes::copy_from_slice(&value.to_be_bytes()))
    });
    let mut files =
        SortedIndexFiles::new(dir.folder(), 0, default_u32_reader(), writer, 10, 0, 2).unwrap();
    for value in [30, 20, 10] {
        files
            .insert(FenseIndex::from_value(value as u64, value))
            .unwrap();
    }
    let before: Vec<_> = (0..files.fragment_count())
        .map(|n| fs::read(files.fragment_path(n)).unwrap())
        .collect();
    fail.set(true);
    assert!(files.compact().is_err());
    let after: Vec<_> = (0..files.fragment_count())
        .map(|n| fs::read(files.fragment_path(n)).unwrap())
        .collect();
    assert_eq!(before, after);
    assert_eq!(
        fs::read_dir(&dir.path).unwrap().count(),
        files.fragment_count()
    );
    assert_eq!(
        pairs(files.all().unwrap()),
        vec![(10, 10), (20, 20), (30, 30)]
    );
}

#[test]
fn randomized_operations_match_an_independent_model_across_restarts() {
    for seed in [1u64, 0x1234, 0xdead_beef] {
        let dir = TestDir::new();
        let mut files = SortedIndexFiles::new(
            dir.folder(),
            0u64,
            default_u64_reader(),
            default_u64_writer(),
            3,
            2,
            7,
        )
        .unwrap();
        let mut state = seed;
        let mut expected = Vec::new();
        for target in 0..200u64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = if target % 31 == 0 {
                u64::MAX
            } else {
                (state >> 32) % 41
            };
            files.insert(FenseIndex::from_value(target, value)).unwrap();
            expected.push((value, target));

            if target % 13 == 0 {
                for num in 0..files.fragment_count() {
                    if let Some(item) = files.read_offset(num, 0).unwrap() {
                        files.clear_offset(num, 0).unwrap();
                        let index = expected
                            .iter()
                            .position(|x| *x == (item.value, item.target))
                            .unwrap();
                        expected.remove(index);
                        break;
                    }
                }
            }
            if target % 29 == 0 {
                files.compact().unwrap();
            }
            if target % 17 == 0 {
                files.flush().unwrap();
                drop(files);
                files = SortedIndexFiles::new(
                    dir.folder(),
                    0u64,
                    default_u64_reader(),
                    default_u64_writer(),
                    3,
                    2,
                    7,
                )
                .unwrap();
                expected.sort();
                assert_eq!(pairs(files.all().unwrap()), expected);
                let range: Vec<_> = expected
                    .iter()
                    .copied()
                    .filter(|x| (10..=30).contains(&x.0))
                    .collect();
                assert_eq!(pairs(files.range(&10, &30).unwrap()), range);
            }
        }
        files.compact().unwrap();
        expected.sort();
        assert_eq!(pairs(files.all().unwrap()), expected);
        assert_fragments(&mut files);
    }
}
