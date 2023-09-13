use crate::format::{DFormat, EFormat};
use std::borrow::Cow;
use std::{mem, ptr};

use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned};

pub type ByteSlice = UnalignedSlice<u8>;

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

/// Describes an [`str`].
pub struct Str;

impl EFormat<'_> for Str {
    type EItem = str;

    fn encode(item: &Self::EItem) -> Cow<'_, [u8]> {
        Cow::Borrowed(item.as_bytes())
    }
}

impl DFormat for Str {
    type DItem = String;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        std::str::from_utf8(bytes).ok().map(|v| v.to_string())
    }
}

pub struct OwnedType<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> EFormat<'a> for OwnedType<T>
    where
        T: AsBytes,
{
    type EItem = T;

    fn encode(item: &'a Self::EItem) -> Cow<[u8]> {
        Cow::Borrowed(<T as AsBytes>::as_bytes(item))
    }
}

impl<'a, T: 'static> DFormat for OwnedType<T>
    where
        T: FromBytes + Copy,
{
    type DItem = T;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        match LayoutVerified::<_, T>::new(bytes) {
            Some(layout) => Some(layout.to_owned()),
            None => {
                let len = bytes.len();
                let elem_size = mem::size_of::<T>();

                // ensure that it is the alignment that is wrong
                // and the length is valid
                if len == elem_size && !aligned_to(bytes, mem::align_of::<T>()) {
                    let mut data = mem::MaybeUninit::<T>::uninit();

                    unsafe {
                        let dst = data.as_mut_ptr() as *mut u8;
                        ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                        return Some(data.assume_init());
                    }
                }

                None
            }
        }
    }
}

pub struct OwnedSlice<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> EFormat<'a> for OwnedSlice<T>
    where
        T: AsBytes,
{
    type EItem = [T];

    fn encode(item: &'a Self::EItem) -> Cow<[u8]> {
        Cow::Borrowed(<[T] as AsBytes>::as_bytes(item))
    }
}

impl<T: 'static> DFormat for OwnedSlice<T>
    where
        T: FromBytes + Copy,
{
    type DItem = Vec<T>;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        match LayoutVerified::<_, [T]>::new_slice(bytes) {
            Some(layout) => Some(layout.to_vec()),
            None => {
                let len = bytes.len();
                let elem_size = mem::size_of::<T>();

                // ensure that it is the alignment that is wrong
                // and the length is valid
                if len % elem_size == 0 && !aligned_to(bytes, mem::align_of::<T>()) {
                    let elems = len / elem_size;
                    let mut vec = Vec::<T>::with_capacity(elems);

                    unsafe {
                        let dst = vec.as_mut_ptr() as *mut u8;
                        ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                        vec.set_len(elems);
                    }

                    return Some(vec);
                }

                None
            }
        }
    }
}

pub struct UnalignedType<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> EFormat<'a> for UnalignedType<T>
    where
        T: AsBytes + Unaligned,
{
    type EItem = T;

    fn encode(item: &'a Self::EItem) -> Cow<[u8]> {
        Cow::Borrowed(<T as AsBytes>::as_bytes(item))
    }
}

impl<T: Clone + 'static> DFormat for UnalignedType<T>
    where
        T: FromBytes + Unaligned,
{
    type DItem = T;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        LayoutVerified::<_, T>::new_unaligned(bytes)
            .map(LayoutVerified::into_ref)
            .map(|v| v.clone())
    }
}

pub struct UnalignedSlice<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> EFormat<'a> for UnalignedSlice<T>
    where
        T: AsBytes + Unaligned,
{
    type EItem = [T];

    fn encode(item: &'a Self::EItem) -> Cow<[u8]> {
        Cow::Borrowed(<[T] as AsBytes>::as_bytes(item))
    }
}

impl<T: Clone + 'static> DFormat for UnalignedSlice<T>
    where
        T: FromBytes + Unaligned,
{
    type DItem = Vec<T>;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        LayoutVerified::<_, [T]>::new_slice_unaligned(bytes)
            .map(LayoutVerified::into_slice)
            .map(|v| v.to_vec())
    }
}

pub struct FixedSlice<T, const N: usize>(std::marker::PhantomData<T>);

impl<'a, T: 'a, const N: usize> EFormat<'a> for FixedSlice<T, N>
    where
        T: AsBytes,
{
    type EItem = [T; N];

    fn encode(item: &'a Self::EItem) -> Cow<[u8]> {
        Cow::Borrowed(<[T] as AsBytes>::as_bytes(item))
    }
}

impl<T: 'static, const N: usize> DFormat for FixedSlice<T, N>
    where
        [T; N]: FromBytes + Default + Copy,
{
    type DItem = [T; N];

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        match LayoutVerified::<_, [T; N]>::new(bytes) {
            Some(v) => Some(v.into_ref().clone()),
            None => {
                assert_eq!(bytes.len(), std::mem::size_of::<[T; N]>());
                let mut out = <[T; N] as Default>::default();

                unsafe {
                    let dst = &mut out as *mut [T; N] as *mut u8;
                    ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
                }
                Some(out)
            }
        }
    }
}

pub struct Split<E, D>(std::marker::PhantomData<(E, D)>);

impl<'e, E, D> EFormat<'e> for Split<E, D>
    where E: EFormat<'e> + 'static,
          D: 'static {
    type EItem = E::EItem;

    fn encode(value: &'e Self::EItem) -> Cow<'e, [u8]> {
        E::encode(value)
    }
}

impl<E, D> DFormat for Split<E, D>
    where E: 'static,
          D: DFormat + 'static {
    type DItem = D::DItem;

    fn decode(data: &[u8]) -> Option<Self::DItem> {
        D::decode(data)
    }
}

#[cfg(feature = "format-protokit")]
pub struct Protokit<T>(std::marker::PhantomData<T>);

#[cfg(feature = "format-protokit")]
impl<'a, T: protokit::BinProto<'a> + 'a> EFormat<'a> for Protokit<T> {
    type EItem = T;

    fn encode(item: &'a Self::EItem) -> Cow<'a, [u8]> {
        protokit::binformat::encode(item).map(Cow::Owned).unwrap()
    }
}

#[cfg(feature = "format-protokit")]
impl<T: for<'a> protokit::BinProto<'a> + 'static + Default> DFormat for Protokit<T> {
    type DItem = T;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        protokit::binformat::decode(bytes).ok()
    }
}


#[cfg(feature = "format-json")]
pub struct SerdeJson<T>(std::marker::PhantomData<T>);

#[cfg(feature = "format-json")]
impl<'a, T: 'a> EFormat<'a> for SerdeJson<T>
    where
        T: serde::Serialize,
{
    type EItem = T;

    fn encode(item: &Self::EItem) -> Cow<[u8]> {
        serde_json::to_vec(item).map(Cow::Owned).unwrap()
    }
}

#[cfg(feature = "format-json")]
impl<T: 'static> DFormat for SerdeJson<T>
    where
        T: serde::de::DeserializeOwned,
{
    type DItem = T;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        serde_json::from_slice(bytes).ok()
    }
}

#[cfg(feature = "format-postcard")]
pub struct Postcard<T>(std::marker::PhantomData<T>);

#[cfg(feature = "format-postcard")]
impl<'a, T: 'a> EFormat<'a> for Postcard<T>
    where
        T: serde::Serialize,
{
    type EItem = T;

    fn encode(item: &Self::EItem) -> Cow<[u8]> {
        postcard::to_allocvec(item).map(Cow::Owned).unwrap()
    }
}

#[cfg(feature = "format-postcard")]
impl<T: 'static> DFormat for Postcard<T>
    where
        T: serde::de::DeserializeOwned,
{
    type DItem = T;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        postcard::from_bytes(bytes).ok()
    }
}

#[cfg(feature = "format-ordcode")]
pub struct Ordcode<T>(std::marker::PhantomData<T>);

#[cfg(feature = "format-ordcode")]
impl<'a, T: 'a> EFormat<'a> for Ordcode<T>
    where
        T: serde::Serialize,
{
    type EItem = T;

    fn encode(item: &Self::EItem) -> Cow<[u8]> {
        ordcode::ser_to_vec_ordered(item, ordcode::Order::Ascending).map(Cow::Owned).unwrap()
    }
}

#[cfg(feature = "format-ordcode")]
impl<T: 'static> DFormat for Ordcode<T>
    where
        T: serde::de::DeserializeOwned,
{
    type DItem = T;

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        ordcode::de_from_bytes_asc(bytes).ok()
    }
}

pub struct Empty;

impl EFormat<'_> for Empty {
    type EItem = ();

    fn encode(_item: &Self::EItem) -> Cow<[u8]> {
        Cow::Borrowed(&[])
    }
}

impl DFormat for Empty {
    type DItem = ();

    fn decode(bytes: &[u8]) -> Option<Self::DItem> {
        if bytes.is_empty() {
            Some(())
        } else {
            None
        }
    }
}

pub struct Ignore;

impl DFormat for Ignore {
    type DItem = ();

    fn decode(_: &[u8]) -> Option<Self::DItem> {
        Some(())
    }
}
