# Measurements

Some RFC rules hold of a duration on the real clock, which no conformance
scenario reads. These measurements give those numbers for roomkit-rs, on the
cases the Python implementation measures in its `benchmarks/media`. They are
run by hand, never by the gates: `make all` and CI only build and lint them.
Each drives the kit with mocks that do no work, never a provider.

| Kind | RFC unit | What it times | The RFC's figure |
|---|---|---|---|
| `close` | RK-12.10.4-056 | `kit.close()` of a kit holding one piece of work it has to cut: a synchronous `BeforeBroadcast` hook, or a model turn's tool call, that honours its cancellation or ignores it | the grace, plus the turn backend's budget, plus the channels' closes, each on its own budget, plus at most two unwinds of 1 s, which only work that ignores its cut spends |

```sh
# Three passes, pinned to one performance core.
cargo build --release -p roomkit-benchmarks
taskset -c 2 target/release/roomkit-benchmarks close
```

A run writes `results.json` (the environment, every pass and every sample)
and `summary.md` into a fresh `--output` directory, by default
`target/benchmark-results/close-<time>/`, and exits non-zero when a case fails
the RFC's figure. A pass records the CPU time other processes spent on the
measured cores meanwhile (`/proc/stat`), and runs again, twice at most, when it
is above 10 % of a core. Quantiles are nearest rank over the passes' samples.

- Each sample builds a fresh kit, with a 50 ms `close_grace`, on a fresh
  multi-thread runtime; the unwinds are the RFC's 1 s.
- Work that honours its cut awaits, so the cut drops it. Work that ignores it
  blocks its thread (`block_in_place`) until the sample releases it, once the
  close returned: a future takes its cut at its next poll, and that one never
  comes while the close runs.
- The bound adds roomkit-rs's budgets: the turn backend in the owner's process
  is given one unwind, an agent's close waits for its turns 5 s at most, the
  transport's close waits for nothing. The sum bounds the close's waits: its
  own work between them (releasing the store, closing a channel that waits for
  nothing) is allowed 50 ms on top (decided 2026-11-27). A case passes when its
  slowest close is within the bound so read and, for work that honours its cut,
  spends no unwind.

## Results

Compare runs from one machine only.

### 2026-11-27, i7-13700KF, rustc 1.99.1

**`close`** (commit 9726bf2f), one performance core (`taskset -c 2`),
Linux 7.0, three passes; other load on the core under 10 % of it in every
pass kept, the second pass run twice more and the third once more for other
load: pass.

| Case | n | p50 (s) | p99 (s) | max (s) | Bound (s) |
|---|---:|---:|---:|---:|---:|
| hook-honours | 60 | 0.051 | 0.053 | 0.053 | 3.05 |
| hook-ignores | 12 | 0.051 | 0.052 | 0.052 | 3.05 |
| tool-honours | 60 | 0.000 | 0.000 | 0.000 | 8.05 |
| tool-ignores | 6 | 8.056 | 8.056 | 8.056 | 8.05 |

A tool that ignores its cut holds the close for every wait in full (the
grace, an unwind, the turn backend's unwind, the agent's 5 s close and the
last unwind) and 6 ms of the close's own work, within the 50 ms it is
allowed. A hook that blocks its thread is not waited for: the close returns
with the grace. The Python implementation, on the same cases, gives such a
hook its two unwinds, and waits one unwind less for such a tool (7.060 s).
Aligning the two is carded (RMK-2162).
