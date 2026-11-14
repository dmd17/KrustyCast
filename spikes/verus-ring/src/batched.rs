//! A verified SPSC ring that moves **batches**: the producer reserves up to
//! `n` free cells with one atomic load, writes them, and publishes them with
//! one atomic store; the consumer takes and releases batches the same way.
//! This amortizes the SeqCst store that `vstd` atomics impose (one `xchg`
//! per batch instead of per element; see REPORT.md).
//!
//! A batch is a contiguous run of cells that does not cross the end of the
//! buffer (callers loop at the wrap point), so ranges stay contiguous in
//! index space. One cell always stays empty, so `head == tail` means empty.
//!
//! Verified: memory safety and data-race freedom (a cell is owned by
//! exactly one side at a time; popped cells were written by a push) and no
//! panics. Not verified: FIFO order of values.
#![allow(unused_imports)]

use std::sync::Arc;
use vstd::atomic_ghost::*;
use vstd::cell::*;
use vstd::map::*;
use vstd::modes::*;
use vstd::prelude::*;
use vstd::set::*;
use vstd::{pervasive::*, *};

verus! {

use verus_state_machines_macros::tokenized_state_machine;

broadcast use vstd::set_lib::range_set_properties, vstd::map::group_map_lemmas, vstd::set::group_set_lemmas,
    vstd::map_lib::group_map_union;

/// Cell `i` holds a value when the ring is `head..tail` (circularly).
pub open spec fn active_at(head: nat, tail: nat, len: nat, i: nat) -> bool {
    i < len && (if head <= tail { head <= i && i < tail } else { i >= head || i < tail })
}

/// Consuming `head..head+n` (all active) only shrinks the active region.
proof fn lemma_consume_shrinks(head: nat, tail: nat, len: nat, n: nat, i: nat)
    requires
        head < len, tail < len, n >= 1, head + n <= len,
        forall|j: nat| head <= j && j < head + n ==> active_at(head, tail, len, j),
        active_at(wrap(head + n, len), tail, len, i),
    ensures
        active_at(head, tail, len, i),
        !(head <= i && i < head + n),
{
    if head <= tail {
        assert(active_at(head, tail, len, (head + n - 1) as nat));
        assert(head + n <= tail);
    } else {
        if head + n < len {
            // still wrapped
        } else {
            assert(head + n == len);
        }
    }
}

/// Outside the consumed range, consuming does not change whether a cell is active.
proof fn lemma_consume_keeps(head: nat, tail: nat, len: nat, n: nat, i: nat)
    requires
        head < len, tail < len, n >= 1, head + n <= len, i < len,
        forall|j: nat| head <= j && j < head + n ==> active_at(head, tail, len, j),
        !(head <= i && i < head + n),
    ensures
        active_at(wrap(head + n, len), tail, len, i) == active_at(head, tail, len, i),
{
    if head <= tail {
        assert(active_at(head, tail, len, (head + n - 1) as nat));
    }
}

/// What produce_start checks makes `tail..tail+n` free.
proof fn lemma_free(head: nat, tail: nat, len: nat, n: nat, i: nat)
    requires
        head < len, tail < len, n >= 1, tail + n <= len,
        if head > tail { tail + n < head } else { tail + n == len ==> head != 0 },
        tail <= i, i < tail + n,
    ensures
        !active_at(head, tail, len, i),
{
}

/// Publishing `tail..tail+n` (all free, not landing on head) grows the
/// active region by exactly those cells.
proof fn lemma_produce_grows(head: nat, tail: nat, len: nat, n: nat, i: nat)
    requires
        head < len, tail < len, n >= 1, tail + n <= len,
        if head > tail { tail + n < head } else { tail + n == len ==> head != 0 },
    ensures
        active_at(head, wrap(tail + n, len), len, i)
            <==> (active_at(head, tail, len, i) || (tail <= i && i < tail + n)),
{
}

pub enum BProducer {
    /// Local copy of the tail.
    Idle(nat),
    /// Cells `start..start + count` checked out for writing.
    Producing(nat, nat),
}

pub enum BConsumer {
    /// Local copy of the head.
    Idle(nat),
    /// Cells `start..start + count` checked out for reading.
    Consuming(nat, nat),
}

pub open spec fn range(start: nat, n: nat) -> Set<nat> {
    Set::<nat>::range(start, start + n)
}

/// An end index `len` wraps to 0.
pub open spec fn wrap(i: nat, len: nat) -> nat {
    if i == len { 0 } else { i }
}

tokenized_state_machine!{BatchQueue<T> {
    fields {
        #[sharding(constant)]
        pub backing_cells: Seq<CellId>,

        #[sharding(storage_map)]
        pub storage: Map<nat, cell::PointsTo<T>>,

        #[sharding(variable)]
        pub head: nat,

        #[sharding(variable)]
        pub tail: nat,

        #[sharding(variable)]
        pub producer: BProducer,

        #[sharding(variable)]
        pub consumer: BConsumer,
    }

    pub open spec fn len(&self) -> nat {
        self.backing_cells.len()
    }

    /// Cells `head..tail` (circularly) hold values.
    pub open spec fn active(&self, i: nat) -> bool {
        active_at(self.head, self.tail, self.len(), i)
    }

    pub open spec fn producing(&self, i: nat) -> bool {
        match self.producer {
            BProducer::Producing(s, n) => s <= i && i < s + n,
            BProducer::Idle(_) => false,
        }
    }

    pub open spec fn consuming(&self, i: nat) -> bool {
        match self.consumer {
            BConsumer::Consuming(s, n) => s <= i && i < s + n,
            BConsumer::Idle(_) => false,
        }
    }

    pub open spec fn valid_storage_at_idx(&self, i: nat) -> bool {
        if self.producing(i) || self.consuming(i) {
            !self.storage.dom().contains(i)
        } else {
            self.storage.dom().contains(i)
            && self.storage.index(i).id() == self.backing_cells.index(i as int)
            && if self.active(i) {
                self.storage.index(i).is_init()
            } else {
                self.storage.index(i).is_uninit()
            }
        }
    }

    #[invariant]
    pub fn valid_storage_all(&self) -> bool {
        forall|i: nat| i < self.len() ==> self.valid_storage_at_idx(i)
    }

    #[invariant]
    pub fn in_bounds(&self) -> bool {
        &&& self.len() > 1
        &&& self.head < self.len()
        &&& self.tail < self.len()
        &&& match self.producer {
            BProducer::Idle(t) => t == self.tail,
            BProducer::Producing(t, n) => t == self.tail && n >= 1 && t + n <= self.len(),
        }
        &&& match self.consumer {
            BConsumer::Idle(h) => h == self.head,
            BConsumer::Consuming(h, n) => h == self.head && n >= 1 && h + n <= self.len(),
        }
    }

    /// The producer's cells are free, and publishing them will not make the
    /// ring look empty; the consumer's cells hold values.
    #[invariant]
    pub fn ranges_ok(&self) -> bool {
        &&& match self.producer {
            BProducer::Producing(t, n) => {
                // Exactly the condition produce_start checked; head may have
                // moved on since, which only frees more cells.
                &&& if self.head > t { t + n < self.head } else { t + n == self.len() ==> self.head != 0 }
            }
            BProducer::Idle(_) => true,
        }
        &&& match self.consumer {
            BConsumer::Consuming(h, n) => forall|i: nat| h <= i && i < h + n ==> self.active(i),
            BConsumer::Idle(_) => true,
        }
    }

    init!{
        initialize(backing_cells: Seq<CellId>, storage: Map<nat, cell::PointsTo<T>>) {
            require(
                forall|i: nat| i < backing_cells.len() ==>
                    #[trigger] storage.dom().contains(i)
                    && storage.index(i).id() == backing_cells.index(i as int)
                    && storage.index(i).is_uninit()
            );
            require(backing_cells.len() > 1);
            init backing_cells = backing_cells;
            init storage = storage;
            init head = 0;
            init tail = 0;
            init producer = BProducer::Idle(0);
            init consumer = BConsumer::Idle(0);
        }
    }

    transition!{
        produce_start(n: nat) {
            require(pre.producer is Idle);
            let tail = pre.producer->Idle_0;
            // Cells tail..tail+n are free, and publishing them does not
            // land on head (one cell stays empty).
            require(n >= 1);
            require(if pre.head > tail {
                tail + n < pre.head
            } else {
                tail + n <= pre.backing_cells.len() && (tail + n == pre.backing_cells.len() ==> pre.head != 0)
            });
            update producer = BProducer::Producing(tail, n);
            birds_eye let perms = pre.storage.restrict(range(tail, n));
            withdraw storage -= (perms) by {
                assert forall|i: nat| perms.dom().contains(i) implies
                    pre.storage.dom().contains(i) && pre.storage.index(i) == perms.index(i)
                by {
                    assert(pre.valid_storage_at_idx(i));
                };
            };
            assert(forall|i: nat| tail <= i && i < tail + n ==>
                #[trigger] perms.dom().contains(i)
                && perms.index(i).id() == pre.backing_cells.index(i as int)
                && perms.index(i).is_uninit()) by {
                assert forall|i: nat| tail <= i && i < tail + n implies
                    #[trigger] perms.dom().contains(i)
                    && perms.index(i).id() == pre.backing_cells.index(i as int)
                    && perms.index(i).is_uninit()
                by {
                    assert(range(tail, n).contains(i));
                    lemma_free(pre.head, tail, pre.backing_cells.len(), n, i);
                    // The consumer holds only active cells, so not this one.
                    match pre.consumer {
                        BConsumer::Consuming(h, m) => {
                            if h <= i && i < h + m {
                                assert(pre.active(i));
                            }
                        }
                        BConsumer::Idle(_) => {}
                    }
                    assert(pre.valid_storage_at_idx(i));
                };
            };
        }
    }

    transition!{
        produce_end(perms: Map<nat, cell::PointsTo<T>>) {
            require(pre.producer is Producing);
            let tail = pre.producer->Producing_0;
            let n = pre.producer->Producing_1;
            require(perms.dom() == range(tail, n));
            require(forall|i: nat| tail <= i && i < tail + n ==>
                (#[trigger] perms.index(i)).id() == pre.backing_cells.index(i as int) && perms.index(i).is_init());
            let next = wrap(tail + n, pre.backing_cells.len());
            update producer = BProducer::Idle(next);
            update tail = next;
            deposit storage += (perms) by {
                assert forall|i: nat| perms.dom().contains(i) implies !pre.storage.dom().contains(i) by {
                    assert(pre.valid_storage_at_idx(i));
                };
            };
        }
    }

    transition!{
        consume_start(n: nat) {
            require(pre.consumer is Idle);
            let head = pre.consumer->Idle_0;
            // Cells head..head+n hold values.
            require(n >= 1);
            require(if head <= pre.tail { head + n <= pre.tail } else { head + n <= pre.backing_cells.len() });
            update consumer = BConsumer::Consuming(head, n);
            birds_eye let perms = pre.storage.restrict(range(head, n));
            withdraw storage -= (perms) by {
                assert forall|i: nat| perms.dom().contains(i) implies
                    pre.storage.dom().contains(i) && pre.storage.index(i) == perms.index(i)
                by {
                    assert(pre.valid_storage_at_idx(i));
                };
            };
            assert(forall|i: nat| head <= i && i < head + n ==>
                #[trigger] perms.dom().contains(i)
                && perms.index(i).id() == pre.backing_cells.index(i as int)
                && perms.index(i).is_init()) by {
                assert forall|i: nat| head <= i && i < head + n implies
                    #[trigger] perms.dom().contains(i)
                    && perms.index(i).id() == pre.backing_cells.index(i as int)
                    && perms.index(i).is_init()
                by {
                    assert(range(head, n).contains(i));
                    // The producer holds only free cells, so not this one.
                    match pre.producer {
                        BProducer::Producing(t, m) => {
                            if t <= i && i < t + m {
                                lemma_free(pre.head, t, pre.backing_cells.len(), m, i);
                            }
                        }
                        BProducer::Idle(_) => {}
                    }
                    assert(pre.valid_storage_at_idx(i));
                };
            };
        }
    }

    transition!{
        consume_end(perms: Map<nat, cell::PointsTo<T>>) {
            require(pre.consumer is Consuming);
            let head = pre.consumer->Consuming_0;
            let n = pre.consumer->Consuming_1;
            require(perms.dom() == range(head, n));
            require(forall|i: nat| head <= i && i < head + n ==>
                (#[trigger] perms.index(i)).id() == pre.backing_cells.index(i as int) && perms.index(i).is_uninit());
            let next = wrap(head + n, pre.backing_cells.len());
            update consumer = BConsumer::Idle(next);
            update head = next;
            deposit storage += (perms) by {
                assert forall|i: nat| perms.dom().contains(i) implies !pre.storage.dom().contains(i) by {
                    assert(pre.valid_storage_at_idx(i));
                };
            };
        }
    }

    property!{
        head_bound() {
            assert(pre.head < pre.backing_cells.len());
        }
    }

    property!{
        tail_bound() {
            assert(pre.tail < pre.backing_cells.len());
        }
    }

    #[inductive(initialize)]
    fn initialize_inductive(post: Self, backing_cells: Seq<CellId>, storage: Map<nat, cell::PointsTo<T>>) {
        assert forall|i: nat| i < post.len() implies post.valid_storage_at_idx(i) by {
            assert(post.storage.dom().contains(i));
        };
    }

    #[inductive(produce_start)]
    fn produce_start_inductive(pre: Self, post: Self, n: nat) {
        let t = pre.tail;
        assert forall|i: nat| i < post.len() implies post.valid_storage_at_idx(i) by {
            assert(pre.valid_storage_at_idx(i));
            if t <= i && i < t + n {
                assert(range(t, n).contains(i));
                lemma_free(pre.head, t, pre.len(), n, i);
                match pre.consumer {
                    BConsumer::Consuming(h, m) => {
                        if h <= i && i < h + m {
                            assert(pre.active(i));
                        }
                    }
                    BConsumer::Idle(_) => {}
                }
            }
        };
        match pre.consumer {
            BConsumer::Consuming(h, m) => {
                assert forall|i: nat| h <= i && i < h + m implies post.active(i) by {
                    assert(pre.active(i));
                };
            }
            BConsumer::Idle(_) => {}
        }
    }

    #[inductive(produce_end)]
    fn produce_end_inductive(pre: Self, post: Self, perms: Map<nat, cell::PointsTo<T>>) {
        let t = pre.tail;
        let n = pre.producer->Producing_1;
        assert forall|i: nat| i < post.len() implies post.valid_storage_at_idx(i) by {
            assert(pre.valid_storage_at_idx(i));
            lemma_produce_grows(pre.head, t, pre.len(), n, i);
            assert(!post.producing(i));
            if t <= i && i < t + n {
                // Newly published: deposited, initialized, now active.
                assert(range(t, n).contains(i));
                assert(perms.dom().contains(i));
                lemma_free(pre.head, t, pre.len(), n, i);
                match pre.consumer {
                    BConsumer::Consuming(h, m) => {
                        if h <= i && i < h + m {
                            assert(pre.active(i));
                        }
                    }
                    BConsumer::Idle(_) => {}
                }
                assert(!post.consuming(i));
                assert(post.storage.dom().contains(i));
                assert(post.storage.index(i) == perms.index(i));
                assert(post.storage.index(i).id() == post.backing_cells.index(i as int));
                assert(post.active(i));
                assert(post.storage.index(i).is_init());
            } else {
                assert(!range(t, n).contains(i));
                assert(!perms.dom().contains(i));
                assert(post.consuming(i) == pre.consuming(i));
                assert(post.active(i) == pre.active(i));
                assert(post.storage.dom().contains(i) == pre.storage.dom().contains(i));
                if pre.storage.dom().contains(i) {
                    assert(post.storage.index(i) == pre.storage.index(i));
                }
            }
        };
        match pre.consumer {
            BConsumer::Consuming(h, m) => {
                assert forall|i: nat| h <= i && i < h + m implies post.active(i) by {
                    assert(pre.active(i));
                    lemma_produce_grows(pre.head, t, pre.len(), n, i);
                };
            }
            BConsumer::Idle(_) => {}
        }
    }

    #[inductive(consume_start)]
    fn consume_start_inductive(pre: Self, post: Self, n: nat) {
        let h = pre.head;
        assert forall|i: nat| i < post.len() implies post.valid_storage_at_idx(i) by {
            assert(pre.valid_storage_at_idx(i));
            if h <= i && i < h + n {
                assert(range(h, n).contains(i));
                match pre.producer {
                    BProducer::Producing(t, m) => {
                        if t <= i && i < t + m {
                            lemma_free(pre.head, t, pre.len(), m, i);
                        }
                    }
                    BProducer::Idle(_) => {}
                }
            }
        };
        assert forall|i: nat| h <= i && i < h + n implies post.active(i) by {};
    }

    #[inductive(consume_end)]
    fn consume_end_inductive(pre: Self, post: Self, perms: Map<nat, cell::PointsTo<T>>) {
        let h = pre.head;
        let n = pre.consumer->Consuming_1;
        let len = pre.len();
        assert forall|j: nat| h <= j && j < h + n implies active_at(h, pre.tail, len, j) by {
            assert(pre.active(j));
        };
        // The consumed range ends at or before tail (or at the end of the buffer).
        if h <= pre.tail {
            assert(active_at(h, pre.tail, len, (h + n - 1) as nat));
        }
        // The producer's reservation still holds against the new head.
        match pre.producer {
            BProducer::Producing(t, m) => {
                if h > t {
                    assert(t + m < h);
                } else {
                    assert(active_at(h, pre.tail, len, (h + n - 1) as nat));
                    assert(h + n <= t);
                }
            }
            BProducer::Idle(_) => {}
        }
        assert forall|i: nat| i < post.len() implies post.valid_storage_at_idx(i) by {
            assert(pre.valid_storage_at_idx(i));
            assert(!post.consuming(i));
            assert(post.producing(i) == pre.producing(i));
            if h <= i && i < h + n {
                // Just released: deposited, empty, no longer active.
                assert(range(h, n).contains(i));
                assert(perms.dom().contains(i));
                if post.active(i) {
                    lemma_consume_shrinks(h, pre.tail, len, n, i);
                }
                assert(!post.active(i));
                match pre.producer {
                    BProducer::Producing(t, m) => {
                        if t <= i && i < t + m {
                            lemma_free(pre.head, t, len, m, i);
                            assert(pre.active(i));
                        }
                    }
                    BProducer::Idle(_) => {}
                }
                assert(!post.producing(i));
                assert(post.storage.dom().contains(i));
                assert(post.storage.index(i) == perms.index(i));
                assert(pre.consumer->Consuming_0 == h);
                assert(perms.index(i).is_uninit());
                assert(post.storage.index(i).is_uninit());
            } else {
                assert(!range(h, n).contains(i));
                assert(!perms.dom().contains(i));
                if post.active(i) {
                    lemma_consume_shrinks(h, pre.tail, len, n, i);
                }
                // Outside the consumed range, activity is unchanged.
                lemma_consume_keeps(h, pre.tail, len, n, i);
                assert(post.active(i) == pre.active(i));
                assert(post.storage.dom().contains(i) == pre.storage.dom().contains(i));
                if pre.storage.dom().contains(i) {
                    assert(post.storage.index(i) == pre.storage.index(i));
                }
            }
        };
    }
}}

/// Free cells after `tail` that one batch may fill (stopping at the end of
/// the buffer and keeping one cell empty), at most `want`.
pub open spec fn free_run(head: nat, tail: nat, len: nat, want: nat) -> nat {
    let avail = if head > tail {
        (head - tail - 1) as nat
    } else if head == 0 {
        (len - tail - 1) as nat
    } else {
        (len - tail) as nat
    };
    if want < avail { want } else { avail }
}

/// Filled cells from `head` that one batch may take (stopping at the end of
/// the buffer), at most `want`.
pub open spec fn full_run(head: nat, tail: nat, len: nat, want: nat) -> nat {
    let avail = if head <= tail { (tail - head) as nat } else { (len - head) as nat };
    if want < avail { want } else { avail }
}

fn free_run_exec(head: u64, tail: usize, len: usize, want: usize) -> (n: usize)
    requires
        tail < len,
        (head as int) < len,
    ensures
        n == free_run(head as nat, tail as nat, len as nat, want as nat),
{
    let h = head as usize;
    let avail = if h > tail {
        h - tail - 1
    } else if h == 0 {
        len - tail - 1
    } else {
        len - tail
    };
    if want < avail { want } else { avail }
}

fn full_run_exec(head: usize, tail: u64, len: usize, want: usize) -> (n: usize)
    requires
        head < len,
        (tail as int) < len,
    ensures
        n == full_run(head as nat, tail as nat, len as nat, want as nat),
{
    let t = tail as usize;
    let avail = if head <= t { t - head } else { len - head };
    if want < avail { want } else { avail }
}

struct_with_invariants!{
    pub struct BRing<T> {
        buffer: Vec<PCell<T>>,
        head: AtomicU64<_, BatchQueue::head<T>, _>,
        tail: AtomicU64<_, BatchQueue::tail<T>, _>,
        instance: Tracked<BatchQueue::Instance<T>>,
    }

    pub closed spec fn wf(&self) -> bool {
        predicate {
            &&& self.instance@.backing_cells().len() == self.buffer@.len()
            &&& forall|i: int| 0 <= i && i < self.buffer@.len() as int ==>
                self.instance@.backing_cells().index(i) == self.buffer@.index(i).id()
        }

        invariant on head with (instance) is (v: u64, g: BatchQueue::head<T>) {
            &&& g.instance_id() == instance@.id()
            &&& g.value() == v as int
        }

        invariant on tail with (instance) is (v: u64, g: BatchQueue::tail<T>) {
            &&& g.instance_id() == instance@.id()
            &&& g.value() == v as int
        }
    }
}

pub struct BatchProducer<T> {
    ring: Arc<BRing<T>>,
    tail: usize,
    producer: Tracked<BatchQueue::producer<T>>,
}

impl<T> BatchProducer<T> {
    pub closed spec fn wf(&self) -> bool {
        (*self.ring).wf()
            && self.producer@.instance_id() == (*self.ring).instance@.id()
            && self.producer@.value() == BProducer::Idle(self.tail as nat)
            && (self.tail as int) < (*self.ring).buffer@.len()
    }
}

pub struct BatchConsumer<T> {
    ring: Arc<BRing<T>>,
    head: usize,
    consumer: Tracked<BatchQueue::consumer<T>>,
}

impl<T> BatchConsumer<T> {
    pub closed spec fn wf(&self) -> bool {
        (*self.ring).wf()
            && self.consumer@.instance_id() == (*self.ring).instance@.id()
            && self.consumer@.value() == BConsumer::Idle(self.head as nat)
            && (self.head as int) < (*self.ring).buffer@.len()
    }
}

/// A ring of `len` cells (holds up to `len - 1` values).
pub fn new_batch_ring<T>(len: usize) -> (pc: (BatchProducer<T>, BatchConsumer<T>))
    requires
        len > 1,
    ensures
        pc.0.wf(),
        pc.1.wf(),
{
    let mut cells = Vec::<PCell<T>>::new();
    let tracked mut perms = Map::<nat, cell::PointsTo<T>>::tracked_empty();
    while cells.len() < len
        invariant
            cells.len() <= len,
            forall|j: nat|
                #![trigger perms.dom().contains(j)]
                #![trigger cells@.index(j as int)]
                #![trigger perms.index(j)]
                j < cells.len() ==> perms.dom().contains(j)
                    && cells@.index(j as int).id() == perms.index(j).id()
                    && perms.index(j).is_uninit(),
        decreases len - cells.len(),
    {
        let ghost i = cells.len();
        let (cell, perm) = PCell::empty();
        cells.push(cell);
        proof {
            perms.tracked_insert(i as nat, perm.get());
        }
    }
    let ghost ids = Seq::<CellId>::new(cells@.len(), |i: int| cells@.index(i).id());
    let tracked (
        Tracked(instance),
        Tracked(head_token),
        Tracked(tail_token),
        Tracked(producer_token),
        Tracked(consumer_token),
    ) = BatchQueue::Instance::initialize(ids, perms, perms);
    let tracked_inst: Tracked<BatchQueue::Instance<T>> = Tracked(instance.clone());
    let head = AtomicU64::new(Ghost(tracked_inst), 0, Tracked(head_token));
    let tail = AtomicU64::new(Ghost(tracked_inst), 0, Tracked(tail_token));
    let ring = Arc::new(BRing::<T> { instance: Tracked(instance), head, tail, buffer: cells });
    (
        BatchProducer::<T> { ring: ring.clone(), tail: 0, producer: Tracked(producer_token) },
        BatchConsumer::<T> { ring, head: 0, consumer: Tracked(consumer_token) },
    )
}

impl<T: Copy> BatchProducer<T> {
    /// Pushes as many of `items` as fit in one batch (all of them if there
    /// is room before the end of the buffer); returns how many. One atomic
    /// load and one atomic store per call.
    pub fn push_batch(&mut self, items: &[T]) -> (n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            n <= items.len(),
    {
        let ring = &*self.ring;
        let len = ring.buffer.len();
        let tail = self.tail;
        let tracked mut reserved = Map::<nat, cell::PointsTo<T>>::tracked_empty();
        let head = atomic_with_ghost!(&ring.head => load();
            returning head;
            ghost head_token => {
                ring.instance.borrow().head_bound(&head_token);
                let ghost n = free_run(head as nat, tail as nat, len as nat, items.len() as nat);
                if n >= 1 {
                    let tracked (_, Tracked(p)) = ring.instance.borrow().produce_start(n, &head_token, self.producer.borrow_mut());
                    reserved = p;
                }
            }
        );
        let n = free_run_exec(head, tail, len, items.len());
        if n == 0 {
            return 0;
        }
        assert(reserved.dom().contains(tail as nat));
        assert(forall|j: nat| tail <= j && j < tail + n ==> #[trigger] reserved.dom().contains(j));
        let tracked mut written = Map::<nat, cell::PointsTo<T>>::tracked_empty();
        let mut i: usize = 0;
        while i < n
            invariant
                ring == &*self.ring,
                (*self.ring).wf(),
                len == ring.buffer@.len(),
                i <= n,
                n <= items.len(),
                tail + n <= len,
                self.producer@.instance_id() == ring.instance@.id(),
                self.producer@.value() == BProducer::Producing(tail as nat, n as nat),
                forall|j: nat| tail + i <= j && j < tail + n ==>
                    #[trigger] reserved.dom().contains(j)
                    && reserved.index(j).id() == ring.instance@.backing_cells().index(j as int)
                    && reserved.index(j).is_uninit(),
                written.dom() == range(tail as nat, i as nat),
                forall|j: nat| tail <= j && j < tail + i ==>
                    (#[trigger] written.index(j)).id() == ring.instance@.backing_cells().index(j as int)
                    && written.index(j).is_init(),
            decreases n - i,
        {
            assert(ring.instance@.backing_cells().index((tail + i) as int)
                == ring.buffer@.index((tail + i) as int).id());
            assert(reserved.dom().contains((tail + i) as nat));
            let tracked mut p = reserved.tracked_remove((tail + i) as nat);
            ring.buffer[tail + i].put(Tracked(&mut p), items[i]);
            proof {
                written.tracked_insert((tail + i) as nat, p);
                assert(written.dom() =~= range(tail as nat, (i + 1) as nat));
            }
            i += 1;
        }
        let next = if tail + n == len { 0 } else { tail + n };
        atomic_with_ghost!(&ring.tail => store(next as u64); ghost tail_token => {
            ring.instance.borrow().produce_end(written, written, &mut tail_token, self.producer.borrow_mut());
        });
        self.tail = next;
        n
    }
}

impl<T: Copy> BatchConsumer<T> {
    /// Pops up to `max` values in one batch (stopping at the end of the
    /// buffer) and appends them to `out`; returns how many.
    pub fn pop_batch(&mut self, out: &mut Vec<T>, max: usize) -> (n: usize)
        requires
            old(self).wf(),
        ensures
            final(self).wf(),
            n <= max,
    {
        let ring = &*self.ring;
        let len = ring.buffer.len();
        let head = self.head;
        let tracked mut taken = Map::<nat, cell::PointsTo<T>>::tracked_empty();
        let tail = atomic_with_ghost!(&ring.tail => load();
            returning tail;
            ghost tail_token => {
                ring.instance.borrow().tail_bound(&tail_token);
                let ghost n = full_run(head as nat, tail as nat, len as nat, max as nat);
                if n >= 1 {
                    let tracked (_, Tracked(p)) = ring.instance.borrow().consume_start(n, &tail_token, self.consumer.borrow_mut());
                    taken = p;
                }
            }
        );
        let n = full_run_exec(head, tail, len, max);
        if n == 0 {
            return 0;
        }
        assert(taken.dom().contains(head as nat));
        assert(forall|j: nat| head <= j && j < head + n ==> #[trigger] taken.dom().contains(j));
        let tracked mut emptied = Map::<nat, cell::PointsTo<T>>::tracked_empty();
        let mut i: usize = 0;
        while i < n
            invariant
                ring == &*self.ring,
                (*self.ring).wf(),
                len == ring.buffer@.len(),
                i <= n,
                head + n <= len,
                self.consumer@.instance_id() == ring.instance@.id(),
                self.consumer@.value() == BConsumer::Consuming(head as nat, n as nat),
                forall|j: nat| head + i <= j && j < head + n ==>
                    #[trigger] taken.dom().contains(j)
                    && taken.index(j).id() == ring.instance@.backing_cells().index(j as int)
                    && taken.index(j).is_init(),
                emptied.dom() == range(head as nat, i as nat),
                forall|j: nat| head <= j && j < head + i ==>
                    (#[trigger] emptied.index(j)).id() == ring.instance@.backing_cells().index(j as int)
                    && emptied.index(j).is_uninit(),
            decreases n - i,
        {
            assert(ring.instance@.backing_cells().index((head + i) as int)
                == ring.buffer@.index((head + i) as int).id());
            assert(taken.dom().contains((head + i) as nat));
            let tracked mut p = taken.tracked_remove((head + i) as nat);
            let v = ring.buffer[head + i].take(Tracked(&mut p));
            out.push(v);
            proof {
                emptied.tracked_insert((head + i) as nat, p);
                assert(emptied.dom() =~= range(head as nat, (i + 1) as nat));
            }
            i += 1;
        }
        let next = if head + n == len { 0 } else { head + n };
        atomic_with_ghost!(&ring.head => store(next as u64); ghost head_token => {
            ring.instance.borrow().consume_end(emptied, emptied, &mut head_token, self.consumer.borrow_mut());
        });
        self.head = next;
        n
    }
}

} // verus!
