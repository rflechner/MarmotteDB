use std::cmp::Ordering;

pub struct FenseIndex<T: Ord> {
    pub tombstone: bool,
    pub target: u64,
    pub value: T,
}

trait SortedIndexTableFragment<T: Ord> {
    fn insert(&mut self, ix: FenseIndex<T>);
    
    fn flag_tombstone(&mut self, target: u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_index_should_be_greater() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { tombstone: false, target: 1, value: "aaaa".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { tombstone: false, target: 2, value: "bbbb".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Greater, r);
    }

    #[test]
    fn string_index_should_be_less() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { tombstone: false, target: 1, value: "zzzz".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { tombstone: false, target: 2, value: "bbbb".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Less, r);
    }

    #[test]
    fn string_index_should_be_equal() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { tombstone: false, target: 1, value: "ddd".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { tombstone: false, target: 2, value: "ddd".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Equal, r);
    }

    #[test]
    fn u64_index_should_be_greater() {
        // given
        let ix1: FenseIndex<u64> = { FenseIndex { tombstone: false, target: 1, value: 45 } };
        let ix2: FenseIndex<u64> = { FenseIndex { tombstone: false, target: 2, value: 60 } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Greater, r);
    }

}
