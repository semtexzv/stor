pub mod db;
pub mod format;
pub mod types;

use crate::format::{DFormat, EFormat};
use std::error::Error;
use std::marker;
use std::mem::ManuallyDrop;
use std::ops::{Deref, RangeBounds};

pub(crate) fn advance_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 255) | None => bytes.push(0),
        Some(last) => *last += 1,
    }
}

pub(crate) fn retreat_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 0) => {
            bytes.pop();
        }
        Some(last) => *last -= 1,
        None => panic!("Vec is empty and must not be"),
    }
}

pub type TableOf<'s, S> = <S as Store>::Table<'s>;
pub type ErrorOf<S> = <S as Store>::Error;

pub type RtxOf<'e, S> = <S as Store>::Rtx<'e>;
pub type WtxOf<'e, S> = <S as Store>::Wtx<'e>;

pub type RangeOf<'e, 'r, S, KC, DC> = <<S as Store>::Table<'e> as Table<'e>>::Range<'r, KC, DC>;
pub type RevRangeOf<'e, 'r, S, KC, DC> =
<<S as Store>::Table<'e> as Table<'e>>::RevRange<'r, KC, DC>;

pub trait Store: Sized + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    type Rtx<'e>: Transaction<Self>
        where
            Self: 'e;

    type Wtx<'e>: Transaction<Self> + Deref<Target=Self::Rtx<'e>>
        where
            Self: 'e;

    type Table<'store>: Table<'store, Store=Self> + Send + Sync
        where
            Self: 'store;

    type Config: Default;

    fn table(&self, name: &str, cfg: &Self::Config) -> Result<Self::Table<'_>, Self::Error>;

    fn typed<KC, DC>(
        &self,
        name: &str,
        cfg: &Self::Config,
    ) -> Result<Typed<Self, KC, DC>, Self::Error> {
        Ok(Typed {
            table: self.table(name, cfg)?,
            marker: Default::default(),
        })
    }

    fn rtx(&self) -> Result<Self::Rtx<'_>, Self::Error>;
    fn wtx(&self) -> Result<Self::Wtx<'_>, Self::Error>;

    fn with_rtx<R>(
        &self,
        fun: impl FnOnce(&RtxOf<Self>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        let rtx = self.rtx()?;
        let out = fun(&rtx)?;
        rtx.commit()?;

        Ok(out)
    }

    fn with_wtx<R>(
        &self,
        fun: impl FnOnce(&mut WtxOf<Self>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        let mut wtx = self.wtx()?;
        let out = fun(&mut wtx)?;
        wtx.commit()?;

        Ok(out)
    }
}

pub trait Transaction<S: Store>: Sized {
    fn commit(self) -> Result<(), ErrorOf<S>>;
}

pub trait Table<'store>: 'store {
    type Store: Store<Table<'store>=Self>
        where
            Self: 'store;

    type Range<'e, KC: DFormat, DC: DFormat>: Iterator<
        Item=Result<(KC::DItem, DC::DItem), ErrorOf<Self::Store>>,
    >;

    type RevRange<'e, KC: DFormat, DC: DFormat>: Iterator<
        Item=Result<(KC::DItem, DC::DItem), ErrorOf<Self::Store>>,
    >;

    fn get<'a, 'txn, KC, DC>(
        &self,
        txn: &'txn RtxOf<Self::Store>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, ErrorOf<Self::Store>>
        where
            KC: EFormat<'a>,
            DC: DFormat;

    fn range<'a, 'txn, KC, DC, R>(
        &self,
        txn: &'txn RtxOf<Self::Store>,
        range: &'a R,
    ) -> Result<Self::Range<'txn, KC, DC>, ErrorOf<Self::Store>>
        where
            KC: EFormat<'a> + DFormat,
            DC: DFormat,
            R: RangeBounds<KC::EItem>;

    fn rev_range<'a, 'txn, KC, DC, R>(
        &self,
        txn: &'txn RtxOf<Self::Store>,
        range: &'a R,
    ) -> Result<Self::RevRange<'txn, KC, DC>, ErrorOf<Self::Store>>
        where
            KC: EFormat<'a> + DFormat,
            DC: DFormat,
            R: RangeBounds<KC::EItem>;

    fn len<'txn>(&self, txn: &'txn RtxOf<Self::Store>) -> Result<usize, ErrorOf<Self::Store>>;

    fn put<'a, KC, DC>(
        &self,
        txn: &mut WtxOf<Self::Store>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), ErrorOf<Self::Store>>
        where
            KC: EFormat<'a>,
            DC: EFormat<'a>;

    fn append<'a, KC, DC>(
        &self,
        txn: &mut WtxOf<Self::Store>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), ErrorOf<Self::Store>>
        where
            KC: EFormat<'a>,
            DC: EFormat<'a>;

    fn delete<'a, KC>(
        &self,
        txn: &mut WtxOf<Self::Store>,
        key: &'a KC::EItem,
    ) -> Result<(), ErrorOf<Self::Store>>
        where
            KC: EFormat<'a>;

    fn clear(&self, txn: &mut WtxOf<Self::Store>) -> Result<(), ErrorOf<Self::Store>>;
}

pub struct Typed<'s, S: Store + 's, KC, DC> {
    table: S::Table<'s>,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<'s, S: Store, KC, DC> Clone for Typed<'s, S, KC, DC>
    where
        S::Table<'s>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            marker: Default::default(),
        }
    }
}

impl<'s, S: Store, KC, DC> Typed<'s, S, KC, DC> {
    pub fn get<'a, 'txn>(
        &self,
        txn: &'txn RtxOf<S>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, ErrorOf<S>>
        where
            KC: EFormat<'a>,
            DC: DFormat,
    {
        self.table.get::<KC, DC>(txn, key)
    }

    pub fn range<'a, 'txn, R>(
        &self,
        txn: &'txn RtxOf<S>,
        range: &'a R,
    ) -> Result<RangeOf<'s, 'txn, S, KC, DC>, ErrorOf<S>>
        where
            KC: EFormat<'a> + DFormat,
            DC: DFormat,
            R: RangeBounds<KC::EItem>,
    {
        self.table.range::<KC, DC, R>(txn, range)
    }

    pub fn rev_range<'a, 'txn, R>(
        &self,
        txn: &'txn RtxOf<S>,
        range: &'a R,
    ) -> Result<RevRangeOf<'s, 'txn, S, KC, DC>, ErrorOf<S>>
        where
            KC: EFormat<'a> + DFormat,
            DC: DFormat,
            R: RangeBounds<KC::EItem>,
    {
        self.table.rev_range::<KC, DC, R>(txn, range)
    }

    pub fn len(&self, txn: &RtxOf<S>) -> Result<usize, ErrorOf<S>> {
        self.table.len(txn)
    }

    pub fn put<'a, 'txn>(
        &self,
        txn: &'txn mut WtxOf<S>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), ErrorOf<S>>
        where
            KC: EFormat<'a>,
            DC: EFormat<'a>,
    {
        self.table.put::<KC, DC>(txn, key, data)
    }

    pub fn append<'a, 'txn>(
        &self,
        txn: &'txn mut WtxOf<S>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), ErrorOf<S>>
        where
            KC: EFormat<'a>,
            DC: EFormat<'a>,
    {
        self.table.append::<KC, DC>(txn, key, data)
    }

    pub fn delete<'a, 'txn>(&self, txn: &'txn mut WtxOf<S>, key: &'a KC::EItem) -> Result<(), ErrorOf<S>>
        where
            KC: EFormat<'a>,
    {
        self.table.delete::<KC>(txn, key).map(|_| ())
    }

    pub fn clear(&self, txn: &mut WtxOf<S>) -> Result<(), ErrorOf<S>> {
        self.table.clear(txn)
    }

    pub fn remap_types<KC2, DC2>(self) -> Typed<'s, S, KC2, DC2> {
        Typed {
            table: self.table,
            marker: Default::default(),
        }
    }

    /// Change the key codec type of this uniform database, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> Typed<'s, S, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this uniform database, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> Typed<'s, S, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }
}

pub struct Tables<S: Store, T> {
    pub store: &'static S,
    pub table: ManuallyDrop<T>,
}

impl<S: Store, T> Tables<S, T> {
    pub fn new<F>(store: S, cfg: &S::Config, make: F) -> Result<Tables<S, T>, S::Error>
        where
            F: FnOnce(&'static S, &S::Config) -> Result<T, S::Error>,
    {
        let store = Box::new(store);
        let store = Box::leak::<'static>(store) as &'static S;
        let o = make(&store, cfg)?;

        Ok(Tables {
            store,
            table: ManuallyDrop::new(o),
        })
    }
}

impl<S: Store, T> Deref for Tables<S, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.table.deref()
    }
}

impl<S: Store, T> Drop for Tables<S, T> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.table);
            drop(Box::from_raw(self.store as *const S as *mut S));
        }
    }
}

pub fn readtx<S: Store, T>(
    s: &S,
    fun: impl FnOnce(&RtxOf<S>) -> Result<T, ErrorOf<S>>,
) -> Result<T, ErrorOf<S>> {
    s.with_rtx(fun)
}

pub fn writetx<S: Store, T>(
    s: &S,
    fun: impl FnOnce(&mut WtxOf<S>) -> Result<T, ErrorOf<S>>,
) -> Result<T, ErrorOf<S>> {
    s.with_wtx(fun)
}

/// Run a query in paged mode (start from provided value), and on each iteration overwrite the value
/// from within the method. If the value was not changed in 2 iterations, we consider the paged
/// query done. Useful for progress reporting migrations that use low amount of memory.
pub fn paged<T: Clone + PartialEq, F: FnMut(&mut T) -> Result<(), E>, E>(
    start: T,
    mut fun: F,
) -> Result<(), E> {
    let mut old = start.clone();
    let mut cur = start.clone();
    loop {
        fun(&mut cur)?;
        if old == cur {
            return Ok(());
        }
        old = cur.clone();
    }
}
