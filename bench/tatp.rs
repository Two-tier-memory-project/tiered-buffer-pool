use rand::{rngs::ThreadRng, seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};
use shumai::{config, ShumaiBench};
use two_tree_storage::{
    bench::tatp::{
        get_a_value, FixedLenString, TableAccessInfo, TableCallForwarding, TableSpecialFacility,
        TableSubscriber,
    },
    DBTuple, TableInterface, ThreeTierOneTree, ThreeTreeBtree, TwoTierOneTree, TwoTreeLowerBtree,
    TwoTreeUpperBlindBtree, TwoTreeUpperBtree,
};
use two_tree_storage::{BufferPoolDB, QueryError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemType {
    OneTree,
    TwoTreeLower,
    TwoTreeUpper,
    TwoTreeUpperBlind,
    ThreeTree,
    TwoTierOneTree,
}

pub struct TATPTxnMix {
    pub get_subscriber_data: u64,
    pub get_new_destination: u64,
    pub get_access_data: u64,
    pub update_subscriber_data: u64,
    pub update_location: u64,
    pub insert_call_forwarding: u64,
    pub delete_call_forwarding: u64,
}

impl TATPTxnMix {
    fn check_integrity(&self) {
        assert_eq!(
            self.get_subscriber_data
                + self.get_new_destination
                + self.get_access_data
                + self.update_subscriber_data
                + self.update_location
                + self.insert_call_forwarding
                + self.delete_call_forwarding,
            100
        );
    }
}

impl TATPTxnMix {
    fn gen(&self, rng: &mut ThreadRng) -> TxnType {
        let distributions = [
            (TxnType::GetSubscriberData, self.get_subscriber_data),
            (TxnType::GetNewDestination, self.get_new_destination),
            (TxnType::GetAccessData, self.get_access_data),
            (TxnType::UpdateSubscriberData, self.update_subscriber_data),
            (TxnType::UpdateLocation, self.update_location),
            (TxnType::InsertCallForwarding, self.insert_call_forwarding),
            (TxnType::DeleteCallForwarding, self.delete_call_forwarding),
        ];

        let random_value = rng.gen_range(0..=100);
        let mut cumulative = 0;

        for (txn_type, prob) in distributions.iter() {
            cumulative += prob;
            if random_value < cumulative {
                return *txn_type;
            }
        }

        panic!("Unexpected error in generating TxnType");
    }
}

impl Default for TATPTxnMix {
    fn default() -> Self {
        Self {
            get_subscriber_data: 35,
            get_new_destination: 10,
            get_access_data: 35,
            update_subscriber_data: 2,
            update_location: 14,
            insert_call_forwarding: 2,
            delete_call_forwarding: 2,
        }
    }
}

#[derive(Clone, Copy)]
enum TxnType {
    GetSubscriberData,
    GetNewDestination,
    GetAccessData,
    UpdateSubscriberData,
    UpdateLocation,
    InsertCallForwarding,
    DeleteCallForwarding,
}

#[config(path = "bench/tatp.toml")]
pub struct TATPConfig {
    pub name: String,
    pub threads: Vec<usize>,
    pub time: usize,
    #[matrix]
    pub subscriber_cnt: usize,
    pub repeat: Option<usize>,
    #[matrix]
    pub sut: SystemType,
    #[matrix]
    pub dram_size_mb: usize,
    #[matrix]
    pub remote_size_mb: usize,
}

struct TATPDatabase<Sub, Access, Facility, Call>
where
    Sub: TableInterface<TableSubscriber>,
    Access: TableInterface<TableAccessInfo>,
    Facility: TableInterface<TableSpecialFacility>,
    Call: TableInterface<TableCallForwarding>,
{
    subscriber_cnt: usize,
    _pool: Sub::Pool,
    subscriber: Sub,
    access_info: Access,
    special_facility: Facility,
    call_forwarding: Call,
    rng_a_value: usize,
}

impl<Sub, Access, Facility, Call> ShumaiBench for TATPDatabase<Sub, Access, Facility, Call>
where
    Sub: TableInterface<TableSubscriber>,
    Access: TableInterface<TableAccessInfo, Pool = Sub::Pool>,
    Facility: TableInterface<TableSpecialFacility, Pool = Sub::Pool>,
    Call: TableInterface<TableCallForwarding, Pool = Sub::Pool>,
{
    type Result = usize;

    type Config = TATPConfig;

    fn load(&mut self) -> Option<serde_json::Value> {
        let guard = crossbeam_epoch::pin();
        for s_id in 0..self.subscriber_cnt {
            let subscriber = TableSubscriber::generate(s_id as u32);

            let lock = self.subscriber.lock_mut(&s_id).unwrap();
            self.subscriber.insert(subscriber, &lock, &guard).unwrap();

            let access_info_iter = TableAccessInfo::generate(s_id as u32);
            for ac in access_info_iter {
                let lock = self.access_info.lock_mut(&ac.primary_key()).unwrap();
                self.access_info.insert(ac, &lock, &guard).unwrap();
            }

            let special_facility_iter = TableSpecialFacility::generate(s_id as u32);
            for sf in special_facility_iter {
                let lock = self.special_facility.lock_mut(&sf.primary_key()).unwrap();
                let call_forwarding_iter = TableCallForwarding::generate(&sf);
                for cf in call_forwarding_iter {
                    let cf_lock = self.call_forwarding.lock_mut(&cf.primary_key()).unwrap();
                    self.call_forwarding.insert(cf, &cf_lock, &guard).unwrap();
                }
                self.special_facility.insert(sf, &lock, &guard).unwrap();
            }
        }
        None
    }

    fn run(&self, context: shumai::Context<Self::Config>) -> Self::Result {
        context.wait_for_start();
        let txn_mix = TATPTxnMix::default();
        txn_mix.check_integrity();

        let mut rng = rand::thread_rng();

        let mut txn_cnt = 0;
        while context.is_running() {
            let txn = txn_mix.gen(&mut rng);

            match txn {
                TxnType::GetSubscriberData => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let txn_v = self.get_subscriber_data(s_id as u32);
                    match txn_v {
                        Ok(_) => txn_cnt += 1,
                        Err(QueryError::TupleNotFound) => {
                            unreachable!("tuple must be find!")
                        }
                        Err(_) => {}
                    }
                }
                TxnType::GetAccessData => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let ai_type = rng.gen_range(1..=4);
                    let txn_v = self.get_access_data(s_id as u32, ai_type);
                    match txn_v {
                        Ok(_) | Err(QueryError::TupleNotFound) => txn_cnt += 1,
                        Err(_) => {}
                    }
                }
                TxnType::GetNewDestination => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let sf_type = rng.gen_range(1..=4);
                    let txn_v = self.get_new_destination(s_id as u32, sf_type);
                    match txn_v {
                        Ok(_) | Err(QueryError::TupleNotFound) => txn_cnt += 1,
                        Err(_) => {}
                    }
                }
                TxnType::UpdateLocation => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let vlr_location = rng.gen_range(1..=10);
                    let txn_v = self.update_location(s_id as u32, vlr_location);
                    match txn_v {
                        Ok(_) => txn_cnt += 1,
                        Err(QueryError::TupleNotFound) => {
                            unreachable!("tuple must be find!")
                        }
                        Err(_) => {}
                    }
                }
                TxnType::UpdateSubscriberData => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let sf_type = rng.gen_range(1..=4);
                    let new_data_a = rng.gen_range(0..=255);
                    let bit_rng = rng.gen_bool(0.5);
                    let txn_v =
                        self.update_subscriber_data(s_id as u32, sf_type, new_data_a, bit_rng);
                    match txn_v {
                        Ok(_) | Err(QueryError::TupleNotFound) => txn_cnt += 1,
                        Err(_) => {}
                    }
                }
                TxnType::InsertCallForwarding => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let txn_v = self.insert_call_forwarding(s_id as u32);
                    match txn_v {
                        Ok(_) | Err(QueryError::TupleNotFound) => txn_cnt += 1,
                        Err(_) => {}
                    }
                }
                TxnType::DeleteCallForwarding => {
                    let s_id = self.nu_rand(&mut rng, 1, self.subscriber_cnt);
                    let sf_type = rng.gen_range(1..=4);
                    let start_time = rng.gen_range(0..=16);
                    let txn_v = self.delete_call_forwarding(s_id as u32, sf_type, start_time);
                    match txn_v {
                        Ok(_) | Err(QueryError::TupleNotFound) => txn_cnt += 1,
                        Err(_) => {}
                    }
                }
            }
        }
        txn_cnt
    }

    fn cleanup(&mut self) -> Option<serde_json::Value> {
        None
    }
}

impl<Sub, Access, Facility, Call> TATPDatabase<Sub, Access, Facility, Call>
where
    Sub: TableInterface<TableSubscriber>,
    Access: TableInterface<TableAccessInfo, Pool = Sub::Pool>,
    Facility: TableInterface<TableSpecialFacility, Pool = Sub::Pool>,
    Call: TableInterface<TableCallForwarding, Pool = Sub::Pool>,
{
    fn new(subscriber_cnt: usize, local_mem_byte: usize, remote_mem_byte: usize) -> Self {
        let mut pool = Sub::Pool::new(local_mem_byte, remote_mem_byte);
        let a_value = get_a_value(subscriber_cnt);
        Self {
            subscriber_cnt,
            subscriber: Sub::new(&mut pool),
            access_info: Access::new(&mut pool),
            special_facility: Facility::new(&mut pool),
            call_forwarding: Call::new(&mut pool),
            _pool: pool,
            rng_a_value: a_value,
        }
    }

    fn nu_rand(&self, rng: &mut ThreadRng, x: usize, y: usize) -> usize {
        ((rng.gen_range(0..=self.rng_a_value) | rng.gen_range(x..=y)) % (y - x + 1)) + x
    }

    fn get_subscriber_data(&self, s_id: u32) -> Result<TableSubscriber, QueryError> {
        let guard = crossbeam_epoch::pin();
        let s_id = s_id as usize;
        let lock = self.subscriber.lock(&s_id)?;
        let v = self.subscriber.get(&s_id, &lock, &guard)?;
        Ok(v)
    }

    fn get_new_destination(
        &self,
        s_id: u32,
        sf_type: u8,
    ) -> Result<FixedLenString<15>, QueryError> {
        let guard = crossbeam_epoch::pin();
        let sf_key = TableSpecialFacility::make_primary_key(s_id, sf_type);
        let lock = self.special_facility.lock(&sf_key)?;

        let v = self.special_facility.get(&sf_key, &lock, &guard)?;
        if !v.is_active {
            return Err(QueryError::TupleNotFound);
        }

        let mut rng = rand::thread_rng();
        let possible_start_time = [0, 8, 16];
        let start_time = possible_start_time.choose(&mut rng).unwrap();

        let start_key = TableCallForwarding::make_primary_key(s_id, sf_type, *start_time);

        let mut out_values = Vec::with_capacity(1);
        let scanned = self
            .call_forwarding
            .scan(&start_key, 1, &mut out_values, &guard)?;

        if scanned == 0 {
            return Err(QueryError::TupleNotFound);
        } else {
            return Ok(out_values[0].1.numberx.clone());
        }
    }

    fn get_access_data(&self, s_id: u32, ai_type: u8) -> Result<TableAccessInfo, QueryError> {
        let pk = TableAccessInfo::make_primary_key(s_id, ai_type);
        let lock = self.access_info.lock(&pk)?;
        let guard = crossbeam_epoch::pin();
        let v = self.access_info.get(&pk, &lock, &guard)?;
        Ok(v)
    }

    fn update_subscriber_data(
        &self,
        s_id: u32,
        sf_type: u8,
        new_data_a: u8,
        bit_rng: bool,
    ) -> Result<(), QueryError> {
        let guard = crossbeam_epoch::pin();
        let sf_key = TableSpecialFacility::make_primary_key(s_id, sf_type);
        let s_id = s_id as usize;
        let lock = self.subscriber.lock_mut(&s_id)?;
        let sf_lock = self.special_facility.lock_mut(&sf_key)?;

        let mut sub_v = self.subscriber.get(&s_id, &lock, &guard)?;
        sub_v.set_bit(0, bit_rng);
        self.subscriber
            .update(&s_id, sub_v, &lock, &guard)
            .expect("update failed");

        let mut sf_v = self.special_facility.get(&sf_key, &sf_lock, &guard)?;
        sf_v.data_a = new_data_a;
        self.special_facility
            .update(&sf_key, sf_v, &lock, &guard)
            .expect("update failed");

        Ok(())
    }

    fn update_location(&self, s_id: u32, vlr_location: u32) -> Result<(), QueryError> {
        let guard = crossbeam_epoch::pin();
        let s_id = s_id as usize;
        let lock = self.subscriber.lock_mut(&s_id)?;
        let mut sub_v = self.subscriber.get(&s_id, &lock, &guard)?;
        sub_v.vlr_location = vlr_location;

        self.subscriber
            .update(&s_id, sub_v, &lock, &guard)
            .expect("update failed");
        Ok(())
    }

    fn insert_call_forwarding(&self, s_id: u32) -> Result<(), QueryError> {
        let guard = crossbeam_epoch::pin();

        let s_id = s_id as usize;
        let s_lock = self.subscriber.lock(&s_id)?;

        let sub_v = self.subscriber.get(&s_id, &s_lock, &guard)?;
        let s_id = sub_v.s_id as usize;

        let mut out_values = vec![];
        self.special_facility
            .scan(&s_id, 4, &mut out_values, &guard)?;

        let possible_start_time = [0, 8, 16];
        for v in out_values {
            let mut rng = rand::thread_rng();
            let sf_type = v.1.sf_type;
            let start_time = possible_start_time.choose(&mut rng).unwrap();
            let cf =
                TableCallForwarding::make_with_key(s_id as u32, sf_type, *start_time, &mut rng);
            let cf_lock = self.call_forwarding.lock_mut(&cf.primary_key())?;
            self.call_forwarding.insert(cf, &cf_lock, &guard).unwrap();
        }
        Ok(())
    }

    fn delete_call_forwarding(
        &self,
        s_id: u32,
        sf_type: u8,
        start_time: u8,
    ) -> Result<(), QueryError> {
        let guard = crossbeam_epoch::pin();
        let rng = &mut rand::thread_rng();
        let mut cf_v = TableCallForwarding::make_with_key(s_id, sf_type, start_time, rng);
        cf_v.deleted = true;

        let cf_lock = self.call_forwarding.lock_mut(&cf_v.primary_key())?;
        self.call_forwarding.insert(cf_v, &cf_lock, &guard)?;

        Ok(())
    }
}

const MB: usize = 1024 * 1024;

fn main() {
    let config = TATPConfig::load().expect("Failed to parse config!");

    for c in config {
        let repeat = c.repeat.unwrap_or(3);

        let results = match c.sut {
            SystemType::OneTree => {
                let mut db = TATPDatabase::<
                    ThreeTierOneTree<TableSubscriber>,
                    ThreeTierOneTree<TableAccessInfo>,
                    ThreeTierOneTree<TableSpecialFacility>,
                    ThreeTierOneTree<TableCallForwarding>,
                >::new(
                    c.subscriber_cnt, c.dram_size_mb * MB, c.remote_size_mb * MB
                );

                let rv = shumai::run(&mut db, &c, repeat);
                std::mem::forget(db);
                rv
            }
            SystemType::TwoTreeLower => {
                let mut db = TATPDatabase::<
                    TwoTreeLowerBtree<TableSubscriber>,
                    TwoTreeLowerBtree<TableAccessInfo>,
                    TwoTreeLowerBtree<TableSpecialFacility>,
                    TwoTreeLowerBtree<TableCallForwarding>,
                >::new(
                    c.subscriber_cnt, c.dram_size_mb * MB, c.remote_size_mb * MB
                );

                let rv = shumai::run(&mut db, &c, repeat);
                std::mem::forget(db);
                rv
            }
            SystemType::TwoTreeUpper => {
                let mut db = TATPDatabase::<
                    TwoTreeUpperBtree<TableSubscriber>,
                    TwoTreeUpperBtree<TableAccessInfo>,
                    TwoTreeUpperBtree<TableSpecialFacility>,
                    TwoTreeUpperBtree<TableCallForwarding>,
                >::new(
                    c.subscriber_cnt, c.dram_size_mb * MB, c.remote_size_mb * MB
                );

                let rv = shumai::run(&mut db, &c, repeat);
                std::mem::forget(db);
                rv
            }
            SystemType::TwoTreeUpperBlind => {
                let mut db = TATPDatabase::<
                    TwoTreeUpperBlindBtree<TableSubscriber>,
                    TwoTreeUpperBlindBtree<TableAccessInfo>,
                    TwoTreeUpperBlindBtree<TableSpecialFacility>,
                    TwoTreeUpperBlindBtree<TableCallForwarding>,
                >::new(
                    c.subscriber_cnt, c.dram_size_mb * MB, c.remote_size_mb * MB
                );

                let rv = shumai::run(&mut db, &c, repeat);
                std::mem::forget(db);
                rv
            }
            SystemType::ThreeTree => {
                let mut db = TATPDatabase::<
                    ThreeTreeBtree<TableSubscriber>,
                    ThreeTreeBtree<TableAccessInfo>,
                    ThreeTreeBtree<TableSpecialFacility>,
                    ThreeTreeBtree<TableCallForwarding>,
                >::new(
                    c.subscriber_cnt, c.dram_size_mb * MB, c.remote_size_mb * MB
                );

                let rv = shumai::run(&mut db, &c, repeat);
                std::mem::forget(db);
                rv
            }
            SystemType::TwoTierOneTree => {
                let mut db = TATPDatabase::<
                    TwoTierOneTree<TableSubscriber>,
                    TwoTierOneTree<TableAccessInfo>,
                    TwoTierOneTree<TableSpecialFacility>,
                    TwoTierOneTree<TableCallForwarding>,
                >::new(
                    c.subscriber_cnt, c.dram_size_mb * MB, c.remote_size_mb * MB
                );

                let rv = shumai::run(&mut db, &c, repeat);
                std::mem::forget(db);
                rv
            }
        };

        results.write_json().expect("Failed to write results!");
    }
}
