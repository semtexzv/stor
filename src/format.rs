use std::borrow::Cow;

pub trait DFormat {
    type DItem;

    fn decode(data: &[u8]) -> Option<Self::DItem>;
}

pub trait EFormat<'e>: 'e {
    type EItem: ?Sized;

    fn encode(value: &'e Self::EItem) -> Cow<'e, [u8]>;
}

pub trait Format<'e, D, E = D>: EFormat<'e, EItem = E> + DFormat<DItem = D> {}

