# Fragment-based sorted index

[Version française](sorted-index-table.fr.md)

The implementation is located in
`marmotte-server/src/indexes/sorted_index_table.rs`. It retains the original
principle: `.ix` files that each contain a sorted index, with a maximum capacity,
an insertion shift limit, and compaction of incomplete fragments.

## Server usage

```rust
use crate::indexes::sorted_index_table::{
    default_u32_reader, default_u32_writer, FenseIndex, SortedIndexFiles,
};

fn example() -> Result<(), String> {
    let mut index = SortedIndexFiles::new(
        "indexes/demo/age".to_owned(),
        0u32,                  // bounds of an empty fragment
        default_u32_reader(),
        default_u32_writer(),
        10,                    // incomplete fragment limit
        128,                   // references shifted per insertion limit
        1024,                  // capacity of each fragment
    )?;

    index.insert(FenseIndex::from_value(4096, 25))?;
    index.insert(FenseIndex::from_value(8192, 30))?;
    let matches = index.find(&25)?;
    assert_eq!(matches[0].target, 4096);
    let interval = index.range(&20, &30)?; // inclusive bounds
    assert_eq!(interval.len(), 2);        // in an initially empty directory
    index.flush()?;
    Ok(())
}
```

`target` is an opaque reference to the data, chosen by the caller. The module
does not read documents. The provided codecs support `String`, `u32`, and `u64`.
Other types can implement `Ord + Clone + BinarySizeable` with a compatible reader
and writer.

`FenseIndex::from_value` calculates the size automatically and creates an active
entry. The legacy `new(target, value, size)` constructor remains available; an
incorrect size is rejected when writing. Size is always measured in bytes,
particularly for UTF-8 strings. Empty keys, zero, and the default value are
ordinary keys. Duplicates are preserved, even when their `(value, target)` pair
is identical.

The constructor opens and verifies all existing fragments. Capacity and the
shift threshold must match the stored values. A directory has a single owner:
do not open it from multiple instances or processes simultaneously.

## Insertion and organization

1. Scan the in-memory headers. In each fragment with available space, use binary
   search to find the position in `(value, target)` order.
2. If the number of references to shift does not exceed `shift_threshold`, append
   the value bytes to the end of the file and update the affected suffix of the
   position table. Existing values are not rewritten.
3. If no available fragment is suitable, a full fragment whose bounds strictly
   contain the value can be split. In keeping with the initial experiment,
   strictly greater values go into a new fragment. The remaining values stay in
   the existing fragment together with the new entry.
4. Otherwise, create a fragment. Keys equal to the bounds of a full fragment are
   accepted in another fragment without overflowing it.
5. When the number of non-empty, non-full fragments exceeds
   `max_incomplete_fragments_count`, combine them, sort them, and redistribute
   them into fragments filled to capacity. Already full fragments are excluded
   from this automatic compaction.

The threshold can be zero: only insertions at the end of a fragment avoid
creating or splitting a fragment. The capacity and incomplete fragment limit
must be positive.

File numbers indicate creation order, not a global ordering of values. Ranges may
overlap. `find` and `range` inspect every fragment with matching bounds, then sort
the results. `all` also returns a globally sorted view.

## Fragment access and compaction

- `fragment_count()` and `read_header(num)` expose metadata.
- `read_fragment(num)` reads active entries in slot order.
- `read_offset(num, offset)` reads a logical slot or returns `None`.
- `write_offset(num, entry, offset)` replaces a slot and may leave the fragment
  unsorted. Replacing an entry does not increment the counter.
- `clear_offset(num, offset)` invalidates the slot, refreshes the bounds, and can
  be called repeatedly without decrementing the counter more than once.
- `reorder_indexes(num, prefix, start)` sorts references and removes gaps. In this
  format, `prefix` is `FenseIndex::<T>::get_prefix_binary_size()` and `start` is
  `read_header(num)?.compute_binary_size() as u64`.
- `compact()` combines all entries and reclaims space occupied by obsolete
  values. Empty files at the end of the sequence are deleted; intermediate empty
  files can be reused.
- `flush()` calls `sync_all` on open files.

Logical slots are not stable identifiers: sorting, insertion, splitting, and
compaction can change them. After a low-level modification that leaves gaps or
changes the order, the next relevant insertion or search sorts the fragment
before using binary search. Bounds are refreshed as soon as the modification is
made.

## MRMTIX02 on-disk format

All integers are big-endian. Files are named `00000000.ix`, `00000001.ix`, and so
on, without missing numbers.

The header occupies **24 bytes**:

| Position | Size | Contents |
| --- | --- | --- |
| 0 | 8 | ASCII signature `MRMTIX02` |
| 8 | 4 | Entry capacity (`u32`) |
| 12 | 4 | Number of active entries (`u32`) |
| 16 | 4 | Shift threshold (`u32`) |
| 20 | 4 | Compact and sorted references: 0 or 1 (`u32`) |

The header is followed by `capacity × 21` bytes reserved for slots:

| Position within the slot | Size | Contents |
| --- | --- | --- |
| 0 | 1 | Active: 0 or 1 |
| 1 | 8 | Target (`u64`) |
| 9 | 8 | Absolute position of the value in the file (`u64`) |
| 17 | 4 | Value size in bytes (`u32`) |

An inactive slot contains only zeros. Values begin after this table. Strings are
UTF-8 without an additional prefix: their length is stored in the slot. Numbers
use exactly 4 or 8 bytes. Minimum and maximum values are reconstructed when the
file is opened and then cached; they no longer occupy a variable-sized header.

Legacy experimental files are rejected without being modified. They must be
rebuilt from the original data; there is no automatic migration from a format
whose positions may already have been inconsistent.

On opening, the module verifies the signature, configuration, counters, flags,
value positions and sizes, and ordering when the fragment is marked as sorted.
Truncated data or invalid UTF-8 produces an error. These checks are not a
substitute for a data checksum.

## Performance and limitations at this stage

Positions and bounds remain in memory; keys are read from disk on demand. Opening
the index scans the table and keys to verify them. A search in a sorted fragment
uses binary search and then reads consecutive results. Fragment selection still
scans their in-memory metadata. An insertion writes the new value and the
affected reference suffix, but still rebuilds the reference vector in memory.

Splitting materializes the affected fragment. Automatic compaction materializes
the affected incomplete fragments. `all` and manual compaction may materialize
the entire index in memory. Splits around the inserted value can produce
unbalanced fragments. This version establishes a functional foundation; it does
not claim measured throughput or latency for a production workload.

Ordinary writes do not perform an `fsync` for every insertion. Temporary
compaction files are fully written and synchronized before replacing the
originals; an encoding error during their preparation preserves the originals.
**Operations spanning multiple files are not atomic.** A crash or I/O error while
publishing a split or compaction can leave a partial operation, including
duplicates. Reference and header writes are not transactional either. After an
I/O error, do not continue using the instance as if the operation had been rolled
back. A recovery log and coordination of concurrent writes remain separate areas
of work.

## Validation

```powershell
cargo test --offline --manifest-path marmotte-server/Cargo.toml
```

The tests cover the original cases, variable-length and Unicode strings, gaps
and replacements, duplicates and bounds, splits, the shift threshold, searches
across overlapping ranges, compactions, and reopenings. Deterministic sequences
of pseudo-random operations are compared with an independent in-memory model.
Deliberately truncated or malformed files verify read errors. Test directories
are unique and isolated within the system temporary directory.
