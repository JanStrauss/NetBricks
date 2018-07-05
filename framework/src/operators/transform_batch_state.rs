use common::*;
use headers::EndOffset;
use interface::Packet;
use interface::PacketTx;
use std::marker::PhantomData;
use std::sync::Arc;
use super::act::Act;
use super::Batch;
use super::iterator::*;
use super::packet_batch::PacketBatch;

pub type TransformStateFn<T, M, S> = Box<FnMut(&mut Packet<T, M>, &mut S) + Send>;
pub type ExtractorFn<S> = Box<FnMut() -> S + Send>;

pub struct TransformStateBatch<T, V, S>
    where
        T: EndOffset,
        V: Batch + BatchIterator<Header=T> + Act,
        S: Send
{
    parent: V,
    transformer: TransformStateFn<T, V::Metadata, S>,
    extractor: ExtractorFn<S>,
    applied: bool,
    phantom_t: PhantomData<T>,
}

impl<T, V, S> TransformStateBatch<T, V, S>
    where
        T: EndOffset,
        V: Batch + BatchIterator<Header=T> + Act,
        S: Send
{
    pub fn new(parent: V,
               transformer: TransformStateFn<T, V::Metadata, S>,
               extractor: ExtractorFn<S>) -> TransformStateBatch<T, V, S> {
        TransformStateBatch {
            parent: parent,
            transformer: transformer,
            extractor: extractor,
            applied: false,
            phantom_t: PhantomData,
        }
    }
}

impl<T, V, S> Batch for TransformStateBatch<T, V, S>
    where
        T: EndOffset,
        V: Batch + BatchIterator<Header=T> + Act,
        S: Send
{}

impl<T, V, S> BatchIterator for TransformStateBatch<T, V, S>
    where
        T: EndOffset,
        V: Batch + BatchIterator<Header=T> + Act,
        S: Send

{
    type Header = T;
    type Metadata = <V as BatchIterator>::Metadata;
    #[inline]
    fn start(&mut self) -> usize {
        self.parent.start()
    }

    #[inline]
    unsafe fn next_payload(&mut self, idx: usize) -> Option<PacketDescriptor<T, Self::Metadata>> {
        self.parent.next_payload(idx)
    }
}

impl<T, V, S> Act for TransformStateBatch<T, V, S>
    where
        T: EndOffset,
        V: Batch + BatchIterator<Header=T> + Act,
        S: Send
{
    #[inline]
    fn act(&mut self) {
        if !self.applied {
            self.parent.act();
            let mut s = (self.extractor)();
            {
                let iter = PayloadEnumerator::<T, V::Metadata>::new(&mut self.parent);
                while let Some(ParsedDescriptor { mut packet, .. }) = iter.next(&mut self.parent) {
                    (self.transformer)(&mut packet, &mut s);
                }
            }
            self.applied = true;
        }
    }

    #[inline]
    fn done(&mut self) {
        self.applied = false;
        self.parent.done();
    }

    #[inline]
    fn send_q(&mut self, port: &PacketTx) -> Result<u32> {
        self.parent.send_q(port)
    }

    #[inline]
    fn capacity(&self) -> i32 {
        self.parent.capacity()
    }

    #[inline]
    fn drop_packets(&mut self, idxes: &[usize]) -> Option<usize> {
        self.parent.drop_packets(idxes)
    }

    #[inline]
    fn clear_packets(&mut self) {
        self.parent.clear_packets()
    }

    #[inline]
    fn get_packet_batch(&mut self) -> &mut PacketBatch {
        self.parent.get_packet_batch()
    }

    #[inline]
    fn get_task_dependencies(&self) -> Vec<usize> {
        self.parent.get_task_dependencies()
    }
}