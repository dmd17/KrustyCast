// A Quint translation of spec/tla/Sequencer.tla (the sequencer core and
// client sessions over one atomic log; ARCHITECTURE §8.1, ADR-4), for the
// TLA+-vs-Quint spike (REPORT.md). Same state, actions and invariants;
// differences from the TLA+ are noted where they occur.
module Sequencer {
  const CLIENTS: Set[str]
  const KEYS: Set[str]
  // TLA+ picks this with CHOOSE; Quint has no deterministic choice.
  const DEFAULT_KEY: str
  const MAX_SEQ: int
  const MAX_INC: int
  const MAX_VER: int
  const MAX_LOG: int
  // TLA+ prunes with a state constraint (CONSTRAINT InFlightBound); Quint
  // has none, so the actions that grow `net` or `fb` are guarded instead.
  const MAX_IN_FLIGHT: int

  pure val SEQS = 1.to(MAX_SEQ)
  pure val INCS = 1.to(MAX_INC)
  pure val VERS = 0.to(MAX_VER)

  // An ingress message. `mode` is "u" (unconditional) or "c" (conditional:
  // accepted only if `key` is at version `ver`, which it then advances).
  type Msg = { c: str, inc: int, seq: int, mode: str, key: str, ver: int }
  type Outcome = Acc(int) | Rej(int) | NoOutcome
  // Feedback to a client: the outcome of its message (c, inc, seq).
  type Fb = { c: str, inc: int, seq: int, res: Outcome }
  type MaybeMsg = NoMsg | SomeMsg(Msg)
  type Session = { next: int, res: int -> Outcome }

  var log: List[Msg]                  // every entry appended (the replicated log)
  var sess: (str, int) -> Session     // dedup state per (client, incarnation)
  var ver: str -> int                 // current version of each key
  var out: List[Msg]                  // egress stream; position + 1 = global seqno
  var net: Set[Msg]                   // messages in flight to the sequencer
  var fb: Set[Fb]                     // feedback in flight to clients
  var inc: str -> int                 // each client's current incarnation
  var sent: str -> int                // highest seq sent by the current incarnation
  var msg: str -> (int -> MaybeMsg)   // what it sent, by seq (for resends)
  var got: str -> (int -> Outcome)    // outcomes it has received, by seq

  pure val NO_SESSION: Session = { next: 1, res: SEQS.mapBy(_ => NoOutcome) }

  action init = all {
    log' = [],
    sess' = tuples(CLIENTS, INCS).mapBy(_ => NO_SESSION),
    ver' = KEYS.mapBy(_ => 0),
    out' = [],
    net' = Set(),
    fb' = Set(),
    inc' = CLIENTS.mapBy(_ => 1),
    sent' = CLIENTS.mapBy(_ => 0),
    msg' = CLIENTS.mapBy(_ => SEQS.mapBy(_ => NoMsg)),
    got' = CLIENTS.mapBy(_ => SEQS.mapBy(_ => NoOutcome)),
  }

  // ---------------------------------------------------------------------
  // The state machine: apply one log entry.

  def withReply(e: Msg, res: Outcome): Set[Fb] =
    fb.union(Set({ c: e.c, inc: e.inc, seq: e.seq, res: res }))

  action apply(e: Msg): bool = {
    val k = (e.c, e.inc)
    val s = sess.get(k)
    if (e.seq < s.next) {
      // Already decided: repeat the stored outcome, change nothing.
      val fb2 = withReply(e, s.res.get(e.seq))
      all { fb2.size() <= MAX_IN_FLIGHT, fb' = fb2, sess' = sess, ver' = ver, out' = out }
    } else if (e.seq > s.next) {
      // A gap in this session (the real system NAKs): no state change.
      all { sess' = sess, ver' = ver, out' = out, fb' = fb }
    } else if (e.mode == "u" or ver.get(e.key) == e.ver) {
      val res = Acc(out.length() + 1)
      val fb2 = withReply(e, res)
      all {
        fb2.size() <= MAX_IN_FLIGHT,
        out' = out.append(e),
        ver' = if (e.mode == "c") ver.setBy(e.key, v => v + 1) else ver,
        sess' = sess.set(k, { next: s.next + 1, res: s.res.set(e.seq, res) }),
        fb' = fb2,
      }
    } else {
      val res = Rej(ver.get(e.key))
      val fb2 = withReply(e, res)
      all {
        fb2.size() <= MAX_IN_FLIGHT,
        sess' = sess.set(k, { next: s.next + 1, res: s.res.set(e.seq, res) }),
        fb' = fb2,
        ver' = ver,
        out' = out,
      }
    }
  }

  // The sequencer takes any message in flight (possibly again) and commits it.
  action sequence(e: Msg): bool = all {
    net.contains(e),
    log.length() < MAX_LOG,
    log' = log.append(e),
    apply(e),
    net' = net, inc' = inc, sent' = sent, msg' = msg, got' = got,
  }

  // ---------------------------------------------------------------------
  // Clients.

  action send(c: str, mode: str, key: str, v: int): bool = {
    val m = { c: c, inc: inc.get(c), seq: sent.get(c) + 1, mode: mode, key: key, ver: v }
    val net2 = net.union(Set(m))
    all {
      sent.get(c) < MAX_SEQ,
      mode == "u" implies (key == DEFAULT_KEY and v == 0), // fields unused when unconditional
      net2.size() <= MAX_IN_FLIGHT,
      net' = net2,
      sent' = sent.setBy(c, x => x + 1),
      msg' = msg.setBy(c, ms => ms.set(m.seq, SomeMsg(m))),
      log' = log, sess' = sess, ver' = ver, out' = out, fb' = fb, inc' = inc, got' = got,
    }
  }

  // Resend an unanswered message that is no longer in flight.
  action resend(c: str, s: int): bool = all {
    s <= sent.get(c),
    got.get(c).get(s) == NoOutcome,
    match msg.get(c).get(s) {
      | SomeMsg(m) => all {
          not(net.contains(m)),
          net.size() < MAX_IN_FLIGHT,
          net' = net.union(Set(m)),
        }
      | NoMsg => all { false, net' = net }
    },
    log' = log, sess' = sess, ver' = ver, out' = out, fb' = fb,
    inc' = inc, sent' = sent, msg' = msg, got' = got,
  }

  action receive(f: Fb): bool = all {
    fb.contains(f),
    fb' = fb.exclude(Set(f)),
    got' = if (f.inc == inc.get(f.c)) got.setBy(f.c, g => g.set(f.seq, f.res)) else got,
    log' = log, sess' = sess, ver' = ver, out' = out, net' = net,
    inc' = inc, sent' = sent, msg' = msg,
  }

  // Crash and restart: a new incarnation, nothing remembered. Old messages
  // and feedback stay in flight.
  action restart(c: str): bool = all {
    inc.get(c) < MAX_INC,
    inc' = inc.setBy(c, x => x + 1),
    sent' = sent.set(c, 0),
    msg' = msg.set(c, SEQS.mapBy(_ => NoMsg)),
    got' = got.set(c, SEQS.mapBy(_ => NoOutcome)),
    log' = log, sess' = sess, ver' = ver, out' = out, net' = net, fb' = fb,
  }

  // ---------------------------------------------------------------------
  // The network.

  action dropMsg(m: Msg): bool = all {
    net.contains(m),
    net' = net.exclude(Set(m)),
    log' = log, sess' = sess, ver' = ver, out' = out, fb' = fb,
    inc' = inc, sent' = sent, msg' = msg, got' = got,
  }

  action dropFeedback(f: Fb): bool = all {
    fb.contains(f),
    fb' = fb.exclude(Set(f)),
    log' = log, sess' = sess, ver' = ver, out' = out, net' = net,
    inc' = inc, sent' = sent, msg' = msg, got' = got,
  }

  action step = any {
    all { net != Set(), nondet e = net.oneOf() sequence(e) },
    nondet c = CLIENTS.oneOf()
    nondet mode = Set("u", "c").oneOf()
    nondet key = KEYS.oneOf()
    nondet v = VERS.oneOf()
    send(c, mode, key, v),
    nondet c = CLIENTS.oneOf()
    nondet s = SEQS.oneOf()
    resend(c, s),
    all { fb != Set(), nondet f = fb.oneOf() receive(f) },
    nondet c = CLIENTS.oneOf()
    restart(c),
    all { net != Set(), nondet m = net.oneOf() dropMsg(m) },
    all { fb != Set(), nondet f = fb.oneOf() dropFeedback(f) },
  }

  // ---------------------------------------------------------------------
  // Properties (PRD §5).

  def id(e: Msg): (str, int, int) = (e.c, e.inc, e.seq)
  def idx(l: List[Msg]): Set[int] = l.indices()

  // Every outcome a client can hear: feedback in flight, and what each
  // client recorded.
  def heard: Set[Fb] =
    fb.union(CLIENTS.map(c =>
      SEQS.filter(s => got.get(c).get(s) != NoOutcome)
          .map(s => { c: c, inc: inc.get(c), seq: s, res: got.get(c).get(s) })
    ).flatten())

  // G4: at most one global seqno per (client, incarnation, seq).
  val atMostOnce = idx(out).forall(i => idx(out).forall(j =>
    i != j implies id(out[i]) != id(out[j])))

  // G5: per key, accepted conditional appends claim versions 0, 1, 2, ...
  val conditionalAppend = idx(out).forall(i =>
    out[i].mode == "c" implies
      out[i].ver == idx(out).filter(j => j < i and out[j].mode == "c" and out[j].key == out[i].key).size())

  // G6: one session's messages are sequenced in seq order.
  val sessionFifo = idx(out).forall(i => idx(out).forall(j =>
    (i < j and out[i].c == out[j].c and out[i].inc == out[j].inc) implies out[i].seq < out[j].seq))

  // Every outcome a client hears is true.
  val outcomesTrue = heard.forall(f => {
    def same(e: Msg): bool = id(e) == (f.c, f.inc, f.seq)
    match f.res {
      | Acc(g) => 1 <= g and g <= out.length() and same(out[g - 1])
      | Rej(_) => idx(out).forall(i => not(same(out[i])))
      | NoOutcome => true
    }
  })

  // A resend never gets a different answer.
  val outcomesStable = heard.forall(f1 => heard.forall(f2 =>
    (f1.c, f1.inc, f1.seq) == (f2.c, f2.inc, f2.seq) implies f1.res == f2.res))

  // Only messages a client really sent are sequenced.
  val noInvention = idx(out).forall(i => idx(log).exists(j => log[j] == out[i]))

  // The key versions agree with the egress stream.
  val versionsMatch = KEYS.forall(k =>
    ver.get(k) == idx(out).filter(i => out[i].mode == "c" and out[i].key == k).size())

  val safety = and {
    atMostOnce, conditionalAppend, sessionFifo, outcomesTrue,
    outcomesStable, noInvention, versionsMatch,
  }
}

// The CI configurations of the TLA+ spec.

// SequencerRace.cfg: two clients racing on one key.
module SequencerRace {
  import Sequencer(
    CLIENTS = Set("c1", "c2"), KEYS = Set("k1"), DEFAULT_KEY = "k1",
    MAX_SEQ = 2, MAX_INC = 1, MAX_VER = 0, MAX_LOG = 4, MAX_IN_FLIGHT = 2
  ).*
}

// SequencerRestart.cfg: one client restarting with messages in flight.
module SequencerRestart {
  import Sequencer(
    CLIENTS = Set("c1"), KEYS = Set("k1"), DEFAULT_KEY = "k1",
    MAX_SEQ = 2, MAX_INC = 2, MAX_VER = 1, MAX_LOG = 4, MAX_IN_FLIGHT = 2
  ).*
}
