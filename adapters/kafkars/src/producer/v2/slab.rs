//! The fixed-capacity table of offer groups the client currently owns.

use std::error::Error;

/// A bounded table of in-flight offer groups, allocated once.
///
/// # Why the capacity is the offer budget
///
/// The legacy phases indexed their slot table by record sequence, so the table
/// was as long as the run: ten million records meant ten million `Option`s,
/// almost all of them permanently `None`. Here the index is a slab position
/// that is reused as groups settle, and the capacity is the outstanding-offer
/// budget.
///
/// That bound is exact, not optimistic. The engine admits a group only while
/// `active + count <= budget`, and every group carries at least one offer, so
/// the number of groups the client owns is at most the number of offers it
/// owns, which is at most the budget. A batch of 256 records occupies one
/// entry, so the table is normally almost empty — it is sized for the worst
/// legal case rather than the expected one, because a slab that can overflow
/// is a slab that will.
#[derive(Debug)]
pub(super) struct OfferSlab<T> {
    entries: Vec<Option<T>>,
    reserved: Vec<bool>,
    free: Vec<usize>,
}

impl<T> OfferSlab<T> {
    /// Allocates a slab that can hold `capacity` groups and never grows.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: (0..capacity).map(|_| None).collect(),
            reserved: vec![false; capacity],
            free: (0..capacity).rev().collect(),
        }
    }

    /// Indexes this slab has, which is the offer budget.
    pub(super) fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Claims an index for a group that is about to be offered.
    pub(super) fn reserve(&mut self) -> Result<usize, Box<dyn Error>> {
        let index = self
            .free
            .pop()
            .ok_or("the offer slab is full, which the admission bound is supposed to prevent")?;
        self.reserved[index] = true;
        Ok(index)
    }

    /// Parks a group at a reserved index while the client owns it.
    pub(super) fn store(&mut self, index: usize, value: T) -> Result<(), Box<dyn Error>> {
        let entry = self
            .entries
            .get_mut(index)
            .ok_or("an offer group was stored at an index outside the slab")?;
        if entry.is_some() {
            return Err("an offer group was stored over another group".into());
        }
        *entry = Some(value);
        Ok(())
    }

    /// Removes the group parked at `index`, if one is parked there.
    ///
    /// A completion can name an index whose group is already being polled, so
    /// an empty entry is an ordinary answer rather than an error; only an
    /// index outside the slab is a defect.
    pub(super) fn take(&mut self, index: usize) -> Result<Option<T>, Box<dyn Error>> {
        Ok(self
            .entries
            .get_mut(index)
            .ok_or("a completion referenced an index outside the offer slab")?
            .take())
    }

    /// Returns a reserved index to the free list once its group is finished.
    pub(super) fn release(&mut self, index: usize) -> Result<(), Box<dyn Error>> {
        match self.reserved.get_mut(index) {
            Some(reserved) if *reserved => *reserved = false,
            Some(_) => return Err("an offer slab index was released twice".into()),
            None => return Err("an offer slab index outside the slab was released".into()),
        }
        self.free.push(index);
        Ok(())
    }
}
