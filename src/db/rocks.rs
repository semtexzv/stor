use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, DBIteratorWithThreadMode, Direction, ErrorKind, IteratorMode, MultiThreaded,
    Options, ReadOptions, TransactionDB,
};

use crate::advance_key;
use crate::format::{DFormat, EFormat};
use crate::types::{ByteSlice, Ignore};
use crate::{ErrorOf, RtxOf, Store, Table, Transaction, WtxOf};

pub type DBType = TransactionDB<MultiThreaded>;

impl Store for DBType {
    type Error = rocksdb::Error;
    type Rtx<'e> = RockTxn<'e>;
    type Wtx<'e> = WRockTxn<'e>;
    type Table<'store> = RockTable<'store>;
    type Config = Options;

    fn table(&self, name: &str, opts: &Self::Config) -> Result<Self::Table<'_>, Self::Error> {
        match self.create_cf(name, opts) {
            Ok(..) => {}
            Err(e)
                if e.kind() == ErrorKind::InvalidArgument
                    && e.to_string().contains("Column family already exists") => {}
            Err(e) => return Err(e),
        };
        let cf = self.cf_handle(name).unwrap();
        Ok(RockTable { cf })
    }

    fn rtx(&self) -> Result<Self::Rtx<'_>, Self::Error> {
        Ok(RockTxn {
            tx: self.transaction(),
        })
    }

    fn wtx(&self) -> Result<Self::Wtx<'_>, Self::Error> {
        Ok(WRockTxn {
            db: RockTxn {
                tx: self.transaction(),
            },
        })
    }
}

pub struct WRockTxn<'a> {
    db: RockTxn<'a>,
}

impl<'a> Deref for WRockTxn<'a> {
    type Target = RockTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl Transaction<DBType> for WRockTxn<'_> {
    fn commit(self) -> Result<(), ErrorOf<DBType>> {
        rocksdb::Transaction::commit(self.db.tx)
    }
}

pub struct RockTxn<'a> {
    tx: rocksdb::Transaction<'a, TransactionDB<MultiThreaded>>,
}

impl Transaction<DBType> for RockTxn<'_> {
    fn commit(self) -> Result<(), ErrorOf<DBType>> {
        rocksdb::Transaction::commit(self.tx)
    }
}

#[derive(Clone)]
pub struct RockTable<'store> {
    cf: Arc<BoundColumnFamily<'store>>,
}

unsafe impl<'store> Send for RockTable<'store> {}

unsafe impl<'store> Sync for RockTable<'store> {}

pub struct Iter<'a, KC: DFormat, DC: DFormat> {
    it: DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, DBType>>,
    _p: PhantomData<(KC, DC)>,
}

impl<'a, KC: DFormat, DC: DFormat> Iterator for Iter<'a, KC, DC> {
    type Item = Result<(KC::DItem, DC::DItem), rocksdb::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next()? {
            Ok(v) => {
                return Some(Ok((KC::decode(&v.0).unwrap(), DC::decode(&v.1).unwrap())));
            }
            Err(e) => {
                return Some(Err(e));
            }
        }
    }
}

impl<'store> Table<'store> for RockTable<'store> {
    type Store = DBType;
    type Range<'e, KC: DFormat, DC: DFormat> = Iter<'e, KC, DC>;
    type RevRange<'e, KC: DFormat, DC: DFormat> = Iter<'e, KC, DC>;

    fn get<'a, 'txn, KC, DC>(
        &self,
        txn: &'txn RtxOf<Self::Store>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, ErrorOf<Self::Store>>
    where
        KC: EFormat<'a>,
        DC: DFormat,
    {
        let key = KC::encode(key);
        let opts = ReadOptions::default();
        let data = txn.tx.get_pinned_cf_opt(&self.cf, key, &opts)?;

        Ok(data.and_then(|v| {
            let out = DC::decode(&v);
            out
        }))
    }

    fn range<'a, 'txn, KC, DC, R>(
        &self,
        txn: &'txn RtxOf<Self::Store>,
        range: &'a R,
    ) -> Result<Self::Range<'txn, KC, DC>, ErrorOf<Self::Store>>
    where
        KC: EFormat<'a> + DFormat,
        DC: DFormat,
        R: RangeBounds<KC::EItem>,
    {
        let mut opt = ReadOptions::default();

        match range.end_bound() {
            Bound::Included(i) => {
                let mut v = KC::encode(i).to_vec();
                crate::advance_key(&mut v);
                opt.set_iterate_upper_bound(v);
            }
            Bound::Excluded(i) => {
                opt.set_iterate_upper_bound(KC::encode(i));
            }
            _ => {}
        };

        let it = match range.start_bound() {
            Bound::Included(i) => {
                let k = KC::encode(i).to_vec();
                txn.tx
                    .iterator_cf_opt(&self.cf, opt, IteratorMode::From(&k, Direction::Forward))
            }
            Bound::Excluded(i) => {
                let mut k = KC::encode(i).to_vec();
                advance_key(&mut k);

                txn.tx
                    .iterator_cf_opt(&self.cf, opt, IteratorMode::From(&k, Direction::Forward))
            }
            Bound::Unbounded => txn.tx.iterator_cf_opt(&self.cf, opt, IteratorMode::Start),
        };

        Ok(Iter {
            it,
            _p: Default::default(),
        })
    }

    fn rev_range<'a, 'txn, KC, DC, R>(
        &self,
        txn: &'txn RtxOf<Self::Store>,
        range: &'a R,
    ) -> Result<Self::RevRange<'txn, KC, DC>, ErrorOf<Self::Store>>
    where
        KC: EFormat<'a> + DFormat,
        DC: DFormat,
        R: RangeBounds<KC::EItem>,
    {
        let mut opt = ReadOptions::default();

        match range.start_bound() {
            Bound::Included(i) => {
                let v = KC::encode(i).to_vec();
                opt.set_iterate_lower_bound(v);
            }
            Bound::Excluded(..) => {
                panic!("Excluded lower bound");
            }
            _ => {}
        };

        let it = match range.end_bound() {
            Bound::Included(i) => {
                let k = KC::encode(i);
                txn.tx
                    .iterator_cf_opt(&self.cf, opt, IteratorMode::From(&k, Direction::Reverse))
            }
            Bound::Excluded(i) => {
                let mut k = KC::encode(i).to_vec();
                crate::retreat_key(&mut k);
                txn.tx
                    .iterator_cf_opt(&self.cf, opt, IteratorMode::From(&k, Direction::Reverse))
            }
            Bound::Unbounded => txn.tx.iterator_cf_opt(&self.cf, opt, IteratorMode::End),
        };

        Ok(Iter {
            it,
            _p: Default::default(),
        })
    }

    fn len<'txn>(&self, txn: &'txn RtxOf<Self::Store>) -> Result<usize, ErrorOf<Self::Store>> {
        Ok(txn.tx.iterator(IteratorMode::Start).count())
    }

    fn put<'a, KC, DC>(
        &self,
        txn: &mut WtxOf<Self::Store>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), ErrorOf<Self::Store>>
    where
        KC: EFormat<'a>,
        DC: EFormat<'a>,
    {
        let k = KC::encode(key);
        let v = DC::encode(data);
        txn.tx.put_cf(&self.cf, k, v)?;

        Ok(())
    }

    fn append<'a, KC, DC>(
        &self,
        txn: &mut WtxOf<Self::Store>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), ErrorOf<Self::Store>>
    where
        KC: EFormat<'a>,
        DC: EFormat<'a>,
    {
        self.put::<KC, DC>(txn, key, data)
    }

    fn delete<'a, KC>(
        &self,
        txn: &mut WtxOf<Self::Store>,
        key: &'a KC::EItem,
    ) -> Result<(), ErrorOf<Self::Store>>
    where
        KC: EFormat<'a>,
    {
        let k = KC::encode(key);
        txn.tx.delete_cf(&self.cf, k)?;
        Ok(())
    }

    fn clear(&self, txn: &mut WtxOf<Self::Store>) -> Result<(), ErrorOf<Self::Store>> {
        let items = self
            .range::<ByteSlice, Ignore, _>(txn, &..)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()?;

        for (k, _) in items {
            self.delete::<ByteSlice>(txn, &k)?;
        }

        Ok(())
    }
}
