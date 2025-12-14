
## Slide 1 — Title & Goal (0:00–0:30)

**Roles implemented:**
- **Clients**: submit values
- **Proposers**: coordinate Paxos rounds and batching
- **Acceptors**: run promise/accept logic
- **Learners**: output the learned total order

**Where in code:** `src/main.py` dispatches `client|proposer|acceptor|learner`.

**Say:** We built MultiPaxos with batching and catch-up, and ensured safety under message loss/crash failures.

---

## Slide 2 — System Architecture (0:30–1:10)

**Communication model:** UDP + IP multicast.

**Message flow (high level):**
- Client → Proposers: client request `{client_id, seq_num, value}`
- Proposer → Acceptors: Paxos `1A/2A`
- Acceptor → Proposers + Learners: Paxos `1B/2B` (opt: 2B direct to learners)
- Learners: decide per instance, deliver in order, print values

**Where in code:**
- Multicast sockets: `src/utils.py` (`mcast_sender`, `mcast_receiver`)
- Proposer: `src/proposer.py`
- Acceptor: `src/acceptor.py`
- Learner: `src/learner.py`

**Say:** Everything runs over multicast sockets created from `logs/config.json`.

---

## Slide 3 — Requirements: Roles + Atomic Broadcast (1:10–1:45)

**Requirement:** Implement the four roles and decide a *sequence* of values in a total order.

**What we do:**
- MultiPaxos instances: proposer increments `next_instance` and runs Paxos per instance (`src/proposer.py: run(), run_paxos()`)
- Learner ensures **in-order delivery** using `next_instance` + `buffer` (`src/learner.py: deliver()`)
- Learner prints **only learned values** (one per line) (`src/learner.py: deliver_value()`)

**Say:** Total order is enforced at learners by only delivering instance `i` after `i-1`.

---

## Slide 4 — Safety: Agreement + Integrity + Total Order (1:45–2:25)

**Safety must always hold** even with message loss or crashes.

**Agreement & validity (Paxos rule):**
- Proposer phase 1 collects a majority of promises (1B) and adopts the value with highest `v_rnd` (`src/proposer.py: run_paxos()`)
- Acceptor promises only for `rnd_val >= rnd` and accepts only if `rnd_val >= promised` (`src/acceptor.py: handle_1A(), handle_2A()`)

**Integrity (no duplicates):**
- Client assigns unique `(client_id, seq_num)` (`src/client.py`) and retransmits on timeout.
- Proposer dedups requests via `seen_requests` (`src/proposer.py: collect_batch(), phase1(), phase2()`)
- Learner dedups deliveries with `delivered_reqs` (`src/learner.py: deliver_value()`)

**Total order:**
- Learner buffers decided values by instance and only delivers sequentially (`src/learner.py: buffer, next_instance, deliver()`)

**Say:** Even if packets are duplicated or lost, the Paxos value-selection rule prevents two different decisions for the same instance.

---

## Slide 5 — Liveness assumptions + Multi-proposer requirement (2:25–3:00)

**Requirement:** no leader oracle; support >1 proposer; if they conflict, they keep trying until one succeeds.

**What we do:**
- Multiple proposers can run concurrently; they use unique ballot numbers `{'bal': counter, 'pid': proposer_id}`.
- Proposer treats a Paxos run as failed if Phase 1 or Phase 2 times out (not enough quorum) and retries (`src/proposer.py: phase1(), phase2(), run()`)
- If another proposer wins an instance with a different value, we retry our batch on the next instance (`src/proposer.py: run()`)

**Important correctness note:** Liveness is best-effort in asynchronous networks; safety is guaranteed.

**Say:** Without leader election, we rely on timeouts + retries; eventually one proposer’s ballots win often enough to make progress.

---

## Slide 6 — Crash failures + “no majority => no progress” (3:00–3:25)

**Requirement:** crash failures only; no recovery for acceptors/learners; and if majority of acceptors are killed, progress must stop.

**What we do:**
- All Paxos state is in memory:
  - Acceptor keeps `instances` and `accepted_values` (`src/acceptor.py`)
  - Learner keeps `votes`, `buffer`, `catchup_votes` (`src/learner.py`)
- Proposer requires `majority = (num_acceptors//2)+1`. If majority cannot reply, Phase1/Phase2 time out and Paxos “fails” (no decision) (`src/proposer.py`).

**Say:** With < majority acceptors, no instance reaches a quorum, so no progress—exactly as required.

---

## Slide 7 — Constraints: Multicast-only + No `sleep()` in `src/` (3:25–3:55)

**Requirement:** IP multicast only; no synchrony primitives like `time.sleep()`.

**What we do:**
- All network I/O uses UDP multicast sockets via `mcast_sender/mcast_receiver` (`src/utils.py`).
- In `src/`, we **do not use** `time.sleep()`.
- Instead, we use `select.select()` with timeouts for waiting on sockets:
  - batching window + non-blocking receive (`src/proposer.py: collect_batch(), phase1(), phase2()`)
  - client waits for acks but still retries (`src/client.py`)

**Say:** `select()` lets us implement timeouts without introducing synchrony by sleeping.

---

## Slide 8 — Milestone 3 Optimizations (3:55–4:35)

**Opt 1: 2B direct to learners**
- Acceptors multicast 2B to learners as well as proposers (`src/acceptor.py: handle_2A()`)

**Opt 2: Proposer Phase1 before receiving values**
- When idle (no pending values), proposer pre-runs phase 1 and caches promises (`src/proposer.py: phase1_opt(), prepared_*`)

**Opt 3: Batching**
- Proposer collects up to `BATCH_SIZE` or `BATCH_TIMEOUT` and proposes a list of `{cid,sn,val}` items (`src/proposer.py: collect_batch()`)

**Say:** These reduce critical-path messages and amortize Paxos overhead across many client values.

---

## Slide 9 — Learner catch-up (4:35–4:55)

**Requirement:** late learner can catch up and learn old values.

**What we do:**
- Learner sends `CATCHUP_REQUEST` on startup and also when it detects a gap (`src/learner.py: run(), handle_2B()`)
- Acceptors reply with their `accepted_values` map (`src/acceptor.py: handle_catchup()`)
- Learner aggregates catchup votes and buffers decided instances once majority reports the same value (`src/learner.py: handle_catchup()`)

**Say:** Catch-up is based on acceptor memory state, consistent with crash-only + no stable storage.

---

## Slide 10 — Testing & What to mention in Q&A (4:55–5:00)

**How we validate:**
- Scripts run scenarios with multiple proposers, varying acceptors, message loss, and catch-up (`scripts/run.sh`, `scripts/tests/**`).

**Q&A bullets to be ready for:**
- *When does Paxos fail?* → when Phase1/Phase2 can’t reach a majority before timeout (loss/crash/competition).
- *Where do duplicates come from?* → client retransmits + UDP multicast can duplicate; handled by `(client_id, seq_num)` dedup.
- *No stable storage?* → Paxos correctness state is in memory; latency logs are measurement-only (as suggested by repo README).

---

# Extra speaker notes (optional)

## “Four multicast groups” clarification
- Config and socket join are driven by the provided skeleton approach: `logs/config.json` holds `{ip, port}` tuples and `mcast_receiver()` joins membership by IP while binding to `(ip,port)`.
- If asked strictly about “four different multicast groups,” point out that the harness uses one multicast IP with distinct ports for roles; your implementation follows that interface.

## One known edge case to mention (if asked)
- Learner’s `deliver_value()` assumes the decided item is a dict with `cid/sn/val`. In this codebase the proposer always batches dict items, so that’s the only expected format in normal runs.

## Things that could be improved (we can say that we found them after the delivery)

- Learner.GAP_THRESHOLD must be set to 1. Let's take the example with a gap of 5:
    - If there are in totale k values to be learned and learner is waiting for the (k - 3)th value to deliver and receive the kth value out of order, the learner will not deliver it or buffer it because it will trigger catchup only if receive values from instances number >= of (next_instance_to_deliver + GAP_THREASHOLD). In our case it is waiting for (k-3) + 5, so for k + 2. But receiving that value is impossible since we have just k values to learn.

- CATCHUP_RESPONSE collection by Learner could be optimized:
    - Currently, the learner for each catchup response it receives, it will count for each instance, how many times sees that value (batch in our case) and try to collect a quorum to have the confirmation that for that specific instance, that value has been decided by a majority of acceptor.
    - The results of this operation is still correct, but since in the acceptors we collect in 'accepted_values' only values(baatch) that reached the 2B stages, we dont need a quorum for each istance but just a single CATCHUP_RESPONSE, since agreement is met trought Paxos and no acceptor has a different value(batch) w.r.t to others for an istance.

- We could send the accepted_values from acceptors to proposer to make the proposer discard the values arlready accepted (values are intedified by the client_id and the sequence_number)

N.B : The more critical things to say is for sure the GAP_THRESHOLD since it will compromise agreement on learner (learner 1 could learn all k values and learner 2 not)
