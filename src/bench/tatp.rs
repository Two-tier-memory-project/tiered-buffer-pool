use rand::{rngs::ThreadRng, seq::SliceRandom, Rng};

use crate::DBTuple;

pub fn get_a_value(subscriber_rows: usize) -> usize {
    match subscriber_rows {
        0..=1_000_000 => 65_535,
        1_000_001..=10_000_000 => 1_048_575,
        _ => 2_097_151,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FixedLenString<const N: usize> {
    buf: [u8; N],
}

impl<const N: usize> FixedLenString<N> {
    pub fn from_num(mut num: usize) -> Self {
        let mut buf = [b'0'; N];
        for i in (0..N).rev() {
            buf[i] = b'0' + (num % 10) as u8;
            num /= 10;
        }
        FixedLenString { buf }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSubscriber {
    pub s_id: u32,
    pub sub_nbr: FixedLenString<15>,
    pub bits: u16,
    pub hex: [u8; 5],
    pub byte2: [u8; 10],
    pub msc_location: u32,
    pub vlr_location: u32,
}

impl DBTuple for TableSubscriber {
    fn primary_key(&self) -> usize {
        self.s_id as usize
    }
}

impl TableSubscriber {
    pub fn generate(s_id: u32) -> Self {
        let mut rng = rand::thread_rng();
        let bits = rng.gen::<u16>() & 0b1111111111; // Only keep the last 10 bits
        let hex = [
            rng.gen_range(0..=15),
            rng.gen_range(0..=15),
            rng.gen_range(0..=15),
            rng.gen_range(0..=15),
            rng.gen_range(0..=15),
        ];
        let byte2 = [
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
            rng.gen(),
        ];
        let msc_location = rng.gen();
        let vlr_location = rng.gen();
        TableSubscriber {
            s_id,
            sub_nbr: FixedLenString::from_num(s_id as usize),
            bits,
            hex,
            byte2,
            msc_location,
            vlr_location,
        }
    }

    // Access methods for bit_X fields
    pub fn bit(&self, index: usize) -> u16 {
        if index < 1 || index > 10 {
            panic!("Index out of range for bit. Valid range is 1 to 10.");
        }
        (self.bits >> (index - 1)) & 1
    }

    pub fn set_bit(&mut self, index: usize, value: bool) {
        if index < 1 || index > 10 {
            panic!("Index out of range for bit. Valid range is 1 to 10.");
        }
        if value {
            self.bits |= 1 << (index - 1);
        } else {
            self.bits &= !(1 << (index - 1));
        }
    }

    // Access methods for hex_X fields
    pub fn hex(&self, index: usize) -> u8 {
        if index < 1 || index > 10 {
            panic!("Index out of range for hex. Valid range is 1 to 10.");
        }

        let byte_index = (index - 1) / 2; // Determine which u8 contains the hex_X value
        let is_high_nibble = (index - 1) % 2 == 0; // Determine if it's the high or low nibble

        if is_high_nibble {
            (self.hex[byte_index] & 0xF0) >> 4
        } else {
            self.hex[byte_index] & 0x0F
        }
    }

    pub fn byte2(&self, index: usize) -> u8 {
        if index < 1 || index > 10 {
            panic!("Index out of range for byte2. Valid range is 1 to 10.");
        }
        self.byte2[index - 1]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableAccessInfo {
    s_id: u32,
    ai_type: u8,
    data1: u8,
    data2: u8,
    data3: FixedLenString<3>,
    data4: FixedLenString<5>,
}

impl DBTuple for TableAccessInfo {
    fn primary_key(&self) -> usize {
        ((self.ai_type as usize) << 32) | self.s_id as usize
    }
}

impl TableAccessInfo {
    fn random_string<const LEN: usize>() -> FixedLenString<LEN> {
        let mut rng = rand::thread_rng();
        let chars: Vec<u8> = (b'A'..=b'Z').collect();
        let mut buf = [0u8; LEN];
        for i in 0..LEN {
            buf[i] = *chars.choose(&mut rng).unwrap();
        }
        FixedLenString { buf }
    }

    pub fn make_primary_key(s_id: u32, ai_type: u8) -> usize {
        ((ai_type as usize) << 32) | s_id as usize
    }

    pub fn generate(s_id: u32) -> AccessInfoGenIter {
        let max_ai_type = ((s_id - 1) % 4) + 1;

        AccessInfoGenIter {
            s_id,
            ai_type: 1,
            max_ai_type: max_ai_type as u8,
            rng: rand::thread_rng(),
        }
    }
}

pub struct AccessInfoGenIter {
    s_id: u32,
    ai_type: u8,
    max_ai_type: u8,
    rng: ThreadRng,
}

impl Iterator for AccessInfoGenIter {
    type Item = TableAccessInfo;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ai_type > self.max_ai_type {
            return None;
        }

        let data1 = self.rng.gen::<u8>();
        let data2 = self.rng.gen::<u8>();
        let data3 = TableAccessInfo::random_string();
        let data4 = TableAccessInfo::random_string();

        self.ai_type += 1;

        Some(TableAccessInfo {
            s_id: self.s_id,
            ai_type: self.ai_type - 1, // because we've already incremented it
            data1,
            data2,
            data3,
            data4,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSpecialFacility {
    pub s_id: u32,
    pub sf_type: u8,
    pub is_active: bool,
    pub error_cntrl: u8,
    pub data_a: u8,
    pub data_b: FixedLenString<5>,
}

impl DBTuple for TableSpecialFacility {
    fn primary_key(&self) -> usize {
        ((self.sf_type as usize) << 32) | self.s_id as usize
    }
}

impl TableSpecialFacility {
    pub fn generate(s_id: u32) -> SpecialFacilityGenIter {
        let max_sf_type = ((s_id - 1) % 4) + 1;

        SpecialFacilityGenIter {
            s_id,
            sf_type: 1,
            max_sf_type: max_sf_type as u8,
            rng: rand::thread_rng(),
        }
    }

    pub fn make_primary_key(s_id: u32, sf_type: u8) -> usize {
        ((sf_type as usize) << 32) | s_id as usize
    }

    fn random_string<const LEN: usize>(rng: &mut ThreadRng) -> FixedLenString<LEN> {
        let chars: Vec<u8> = (b'A'..=b'Z').collect();
        let mut buf = [0u8; LEN];
        for i in 0..LEN {
            buf[i] = *chars.choose(rng).unwrap();
        }
        FixedLenString { buf }
    }
}

pub struct SpecialFacilityGenIter {
    s_id: u32,
    sf_type: u8,
    max_sf_type: u8,
    rng: ThreadRng,
}

impl Iterator for SpecialFacilityGenIter {
    type Item = TableSpecialFacility;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sf_type > self.max_sf_type {
            return None;
        }

        let is_active = self.rng.gen_bool(0.85);
        let error_cntrl = self.rng.gen::<u8>();
        let data_a = self.rng.gen::<u8>();
        let data_b = TableSpecialFacility::random_string(&mut self.rng);

        self.sf_type += 1;

        Some(TableSpecialFacility {
            s_id: self.s_id,
            sf_type: self.sf_type - 1, // because we've already incremented it
            is_active,
            error_cntrl,
            data_a,
            data_b,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableCallForwarding {
    s_id: u32,
    sf_type: u8,
    start_time: u8,
    end_time: u8,
    pub numberx: FixedLenString<15>,
    pub deleted: bool, // this is a temp hack, as we don't have real delete yet
}

impl DBTuple for TableCallForwarding {
    fn primary_key(&self) -> usize {
        ((self.s_id as usize) << 32) | ((self.sf_type as usize) << 16) | self.start_time as usize
    }
}

impl TableCallForwarding {
    pub fn make_primary_key(s_id: u32, sf_type: u8, start_time: u8) -> usize {
        ((s_id as usize) << 32) | ((sf_type as usize) << 16) | start_time as usize
    }

    pub fn make_with_key(s_id: u32, sf_type: u8, start_time: u8, rng: &mut ThreadRng) -> Self {
        let end_time = start_time + rng.gen_range(1..=8);
        let numberx = TableCallForwarding::random_numberx(rng);

        TableCallForwarding {
            s_id,
            sf_type,
            start_time,
            end_time,
            numberx,
            deleted: false,
        }
    }

    pub fn generate(facility: &TableSpecialFacility) -> CallForwardingGenIter {
        let mut rng = rand::thread_rng();
        let max_records = match rng.gen_range(0..=3) {
            0 => 0,
            1 => 1,
            2 => 2,
            _ => 3,
        };
        CallForwardingGenIter {
            s_id: facility.s_id,
            sf_type: facility.sf_type,
            remaining_start_times: vec![0, 8, 16],
            max_records,
            current_record: 0,
            rng,
        }
    }

    fn random_numberx(rng: &mut ThreadRng) -> FixedLenString<15> {
        let mut buf = [0u8; 15];
        for i in 0..15 {
            buf[i] = (rng.gen_range(0..10) + b'0') as u8;
        }
        FixedLenString { buf }
    }
}

pub struct CallForwardingGenIter {
    s_id: u32,
    sf_type: u8,
    remaining_start_times: Vec<u8>,
    max_records: usize,
    current_record: usize,
    rng: ThreadRng,
}

impl Iterator for CallForwardingGenIter {
    type Item = TableCallForwarding;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_record >= self.max_records {
            return None;
        }

        let start_time = self.remaining_start_times.pop().unwrap();
        let end_time = start_time + self.rng.gen_range(1..=8);
        let numberx = TableCallForwarding::random_numberx(&mut self.rng);

        self.current_record += 1;

        Some(TableCallForwarding {
            s_id: self.s_id,
            sf_type: self.sf_type,
            start_time,
            end_time,
            numberx,
            deleted: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit() {
        let subscriber = TableSubscriber {
            s_id: 1,
            sub_nbr: FixedLenString { buf: [b'0'; 15] },
            bits: 0b1010101010, // Let's set alternating bits for testing
            hex: [0x12, 0x34, 0x56, 0x78, 0x9A], // Setting known values for testing
            byte2: [0; 10],     // Not testing this here, so initializing to zeros
            msc_location: 0,
            vlr_location: 0,
        };

        assert_eq!(subscriber.bit(1), 0);
        assert_eq!(subscriber.bit(2), 1);
        assert_eq!(subscriber.bit(3), 0);
        assert_eq!(subscriber.bit(4), 1);
        assert_eq!(subscriber.bit(5), 0);
        assert_eq!(subscriber.bit(6), 1);
        assert_eq!(subscriber.bit(7), 0);
        assert_eq!(subscriber.bit(8), 1);
        assert_eq!(subscriber.bit(9), 0);
        assert_eq!(subscriber.bit(10), 1);
    }

    #[test]
    fn test_hex() {
        let subscriber = TableSubscriber {
            s_id: 1,
            sub_nbr: FixedLenString { buf: [b'0'; 15] },
            bits: 0,
            hex: [0x12, 0x34, 0x56, 0x78, 0x9A], // Setting known values for testing
            byte2: [0; 10],                      // Not testing this here, so initializing to zeros
            msc_location: 0,
            vlr_location: 0,
        };

        assert_eq!(subscriber.hex(1), 0x1);
        assert_eq!(subscriber.hex(2), 0x2);
        assert_eq!(subscriber.hex(3), 0x3);
        assert_eq!(subscriber.hex(4), 0x4);
        assert_eq!(subscriber.hex(5), 0x5);
        assert_eq!(subscriber.hex(6), 0x6);
        assert_eq!(subscriber.hex(7), 0x7);
        assert_eq!(subscriber.hex(8), 0x8);
        assert_eq!(subscriber.hex(9), 0x9);
        assert_eq!(subscriber.hex(10), 0xA);
    }

    #[test]
    #[should_panic(expected = "Index out of range for bit. Valid range is 1 to 10.")]
    fn test_bit_out_of_range() {
        let subscriber = TableSubscriber::generate(1);
        subscriber.bit(11); // This should panic
    }

    #[test]
    #[should_panic(expected = "Index out of range for hex. Valid range is 1 to 10.")]
    fn test_hex_out_of_range() {
        let subscriber = TableSubscriber::generate(1);
        subscriber.hex(11); // This should panic
    }

    #[test]
    fn test_access_info_data_ranges() {
        let s_id = 1;
        let records: Vec<_> = TableAccessInfo::generate(s_id).collect();

        for record in &records {
            assert!(record.data3.buf.iter().all(|&b| b >= b'A' && b <= b'Z'));
            assert!(record.data4.buf.iter().all(|&b| b >= b'A' && b <= b'Z'));
        }
    }

    #[test]
    fn test_unique_ai_type() {
        let s_id = 1;
        let records: Vec<_> = TableAccessInfo::generate(s_id).collect();
        let mut ai_types = vec![];

        for record in &records {
            assert!(!ai_types.contains(&record.ai_type));
            ai_types.push(record.ai_type);
        }
    }

    #[test]
    fn test_ai_type_range() {
        let s_id = 1;
        let records: Vec<_> = TableAccessInfo::generate(s_id).collect();

        for record in &records {
            assert!(record.ai_type >= 1 && record.ai_type <= 4);
        }
    }

    #[test]
    fn test_record_count_distribution() {
        let mut counts = [0; 4];
        for s_id in 1..=200 {
            let record_count = TableAccessInfo::generate(s_id).count();
            counts[record_count - 1] += 1;
        }

        // Roughly check if the distribution is close to 25% for each count
        for &count in &counts {
            assert!((40..=60).contains(&count), "Count was: {}", count);
        }
    }

    #[test]
    fn test_special_facility_data_ranges() {
        let s_id = 1;
        let records: Vec<_> = TableSpecialFacility::generate(s_id).collect();

        for record in &records {
            assert!(record.data_b.buf.iter().all(|&b| b >= b'A' && b <= b'Z'));
        }
    }

    #[test]
    fn test_unique_sf_type() {
        let s_id = 1;
        let records: Vec<_> = TableSpecialFacility::generate(s_id).collect();
        let mut sf_types = vec![];

        for record in &records {
            assert!(!sf_types.contains(&record.sf_type));
            sf_types.push(record.sf_type);
        }
    }

    #[test]
    fn test_sf_type_range() {
        let s_id = 1;
        let records: Vec<_> = TableSpecialFacility::generate(s_id).collect();

        for record in &records {
            assert!(record.sf_type >= 1 && record.sf_type <= 4);
        }
    }

    #[test]
    fn test_is_active_distribution() {
        let mut active_count = 0;
        let mut total_count = 0;

        for s_id in 1..=1000 {
            // Using a larger sample for better distribution accuracy
            let records: Vec<_> = TableSpecialFacility::generate(s_id).collect();
            for record in &records {
                if record.is_active {
                    active_count += 1;
                }
                total_count += 1;
            }
        }

        let active_percentage = (active_count as f64 / total_count as f64) * 100.0;
        assert!(
            (83.0..=87.0).contains(&active_percentage),
            "Active percentage was: {}",
            active_percentage
        );
    }

    #[test]
    fn test_tf_record_count_distribution() {
        let mut counts = [0; 4];
        for s_id in 1..=200 {
            let record_count = TableSpecialFacility::generate(s_id).count();
            counts[record_count - 1] += 1;
        }

        // Roughly check if the distribution is close to 25% for each count
        for &count in &counts {
            assert!((40..=60).contains(&count), "Count was: {}", count);
        }
    }

    #[test]
    fn test_call_forwarding_data_ranges() {
        let facility = TableSpecialFacility::generate(1).next().unwrap();
        let records: Vec<_> = TableCallForwarding::generate(&facility).collect();

        for record in &records {
            assert_eq!(record.s_id, facility.s_id);
            assert_eq!(record.sf_type, facility.sf_type);
            assert!([0, 8, 16].contains(&record.start_time));
            assert!(
                record.end_time > record.start_time && record.end_time <= record.start_time + 8
            );
            assert!(record.numberx.buf.iter().all(|&b| b >= b'0' && b <= b'9'));
        }
    }

    #[test]
    fn test_unique_start_time() {
        let facility = TableSpecialFacility::generate(1).next().unwrap();
        let records: Vec<_> = TableCallForwarding::generate(&facility).collect();
        let mut start_times = vec![];

        for record in &records {
            assert!(!start_times.contains(&record.start_time));
            start_times.push(record.start_time);
        }
    }
}
