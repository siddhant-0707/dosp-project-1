# Consecutive Squares Actor System

## Overview

This program finds all starting points where k consecutive squares sum to a perfect square, using Gleam's actor model for parallelism. The system follows a boss-worker pattern where a single boss coordinates multiple worker actors across available CPU cores.

## Architecture

```
┌─────────────────┐
│   Main Process  │
│   (squares.gleam) │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│  Boss Actor     │
│  (boss.gleam)   │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Worker Actor 1 │    │  Worker Actor 2 │    │  Worker Actor N │
│ (worker.gleam)  │    │ (worker.gleam)  │    │ (worker.gleam)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Code Flow

### 1. Main Entry Point (`squares.gleam`)

```gleam
pub fn main() -> Nil {
  let args = argv.load().arguments
  case args {
    [n_str, k_str] -> {
      // Parse command line arguments N and k
      let parse = fn(s: String) { int.parse(s) }
      let res = result.try(parse(n_str), fn(n) {
        result.try(parse(k_str), fn(k) { Ok(#(n, k)) })
      })
      case res {
        Ok(#(n, k)) -> run(n, k)
        Error(_) -> usage()
      }
    }
    _ -> usage()
  }
}
```

**Flow:**
1. Parse command line arguments N (max starting point) and k (sequence length)
2. Call `run(n, k)` to start the computation

### 2. System Initialization (`run` function)

```gleam
fn run(n: Int, k: Int) -> Nil {
  let assert Ok(actor.Started(_, data: subject)) = boss.start()
  let work_unit = pick_work_unit(k)
  let starts = boss.solve(subject, n, k, work_unit)
  print_starts(starts)
}
```

**Flow:**
1. **Start Boss Actor**: Creates the boss actor and gets its message subject
2. **Determine Work Unit Size**: Uses heuristics to decide optimal chunk size
3. **Solve Problem**: Calls boss.solve() which blocks until all work is complete
4. **Print Results**: Outputs all found starting points

### 3. Boss Actor (`boss.gleam`)

The boss actor coordinates the entire computation:

#### Initialization
```gleam
pub fn start() -> actor.StartResult(process.Subject(BossMsg)) {
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      let worker_results_subject = process.new_subject()
      let selector =
        process.new_selector()
        |> process.select(subject)
        |> process.select_map(worker_results_subject, WorkerDone)

      actor
      .initialised(init_state(subject, worker_results_subject))
      |> actor.selecting(selector)
      |> actor.returning(subject)
      |> Ok
    })
    |> actor.on_message(handle)

  actor.start(builder)
}
```

**Key Components:**
- **Message Selector**: Handles both boss messages and worker result messages
- **Worker Results Subject**: Dedicated channel for receiving worker completions
- **State Initialization**: Creates N worker actors (N = number of CPU cores)

#### Work Distribution
```gleam
Solve(reply_to, n, k, work_unit) -> {
  let ranges = make_ranges(n, work_unit)
  let num_ranges = list.length(ranges)
  let new_state =
    State(
      ..state,
      pending: num_ranges,
      reply_to: option.Some(reply_to),
      acc: [],
      k_current: k,
    )
  dispatch_jobs(new_state, ranges)
  actor.continue(new_state)
}
```

**Flow:**
1. **Range Generation**: Splits 1..N into work chunks of size `work_unit`
2. **State Update**: Tracks pending work count and reply destination
3. **Job Dispatch**: Round-robin assigns ranges to available workers
4. **Wait**: Boss waits for all workers to complete

#### Result Aggregation
```gleam
WorkerDone(worker.Done(_, starts)) -> {
  let new_pending = state.pending - 1
  let new_acc = list.append(state.acc, starts)
  case new_pending == 0 {
    True -> {
      case state.reply_to {
        option.Some(r) -> process.send(r, Solved(list.sort(new_acc, by: int.compare)))
        option.None -> Nil
      }
      // Reset state for next computation
    }
    False -> actor.continue(State(..state, pending: new_pending, acc: new_acc))
  }
}
```

**Flow:**
1. **Decrement Counter**: Reduce pending work count
2. **Accumulate Results**: Append worker's findings to master list
3. **Check Completion**: If all work done, sort results and reply to caller
4. **Continue**: Otherwise, wait for more worker completions

### 4. Worker Actors (`worker.gleam`)

Each worker processes a range of starting points:

#### Message Handling
```gleam
Compute(reply_to, from, to, k) -> {
  let starts = find_starts_in_range(from, to, k)
  process.send(reply_to, Done(self_subject, starts))
  actor.continue(self_subject)
}
```

#### Core Algorithm
```gleam
fn find_starts_in_range(from: Int, to: Int, k: Int) -> List(Int) {
  find_starts_loop(from, to, k, [])
}

fn find_starts_loop(cur: Int, to: Int, k: Int, acc: List(Int)) -> List(Int) {
  case cur > to {
    True -> list.reverse(acc)
    False -> {
      let sum = segment_sum_squares(cur, k)
      case is_perfect_square(sum) {
        True -> find_starts_loop(cur + 1, to, k, [cur, ..acc])
        False -> find_starts_loop(cur + 1, to, k, acc)
      }
    }
  }
}
```

**Algorithm Steps:**
1. **For each starting point** in assigned range [from, to]:
2. **Calculate sum**: Sum of k consecutive squares starting at current point
3. **Check perfect square**: Use integer square root to test
4. **Accumulate**: If perfect square, add starting point to results
5. **Reply**: Send results back to boss when range complete

#### Mathematical Optimization

**Sum Formula**: Instead of computing each square individually, uses the mathematical formula:
```gleam
fn segment_sum_squares(start: Int, k: Int) -> Int {
  let end_ = start + k - 1
  sum_squares_upto(end_) - sum_squares_upto(start - 1)
}

fn sum_squares_upto(n: Int) -> Int {
  case n <= 0 {
    True -> 0
    False -> numerator(n) / 6
  }
}

fn numerator(n: Int) -> Int {
  n * {n + 1} * {2 * n + 1}
}
```

This uses the closed form: `Σ(i²) = n(n+1)(2n+1)/6`

## Parallelism Strategy

### 1. **Actor-Based Concurrency**
- **Boss Actor**: Single coordinator, no computational bottleneck
- **Worker Actors**: Multiple (N = CPU cores), handle computation
- **Message Passing**: Asynchronous, non-blocking communication

### 2. **Work Distribution**
- **Range Splitting**: Problem space [1..N] divided into chunks
- **Load Balancing**: Round-robin assignment ensures even distribution
- **Chunk Sizing**: Adaptive based on k value (larger k = larger chunks)

### 3. **Scalability**
```gleam
fn init_state(self, worker_results_subject) -> State {
  let num = sysinfo.schedulers_online()  // Get CPU core count
  let worker_count = case num > 0 { True -> num | False -> 1 }
  let workers = spawn_workers(worker_count, [])
  // ...
}
```

- **Dynamic Core Detection**: Uses `sysinfo.schedulers_online()`
- **Automatic Scaling**: Creates one worker per available core
- **No Over-subscription**: Avoids creating too many actors

### 4. **Performance Characteristics**

**Measured Results:**
- **CPU/Real Time Ratio**: 6.92 (for `squares 500000 24`)
- **Core Utilization**: ~7 cores effectively used
- **CPU Usage**: 690% (indicating strong parallelism)

**Why It Works:**
- **CPU-Bound**: Perfect square checking is computationally intensive
- **Independent Work**: No shared state between workers
- **Minimal Communication**: Workers only report results, no coordination needed
- **Balanced Load**: Even distribution of starting points across workers

## Key Design Decisions

### 1. **Work Unit Sizing**
```gleam
fn pick_work_unit(k: Int) -> Int {
  case k {
    _ if k >= 1000 -> 20000
    _ if k >= 100 -> 10000
    _ if k >= 20 -> 5000
    _ -> 2000
  }
}
```
- **Adaptive**: Larger k values get bigger chunks (more computation per item)
- **Balanced**: Prevents too many small messages vs too few large chunks

### 2. **Integer Square Root**
- **Fast Algorithm**: Uses Newton's method for O(log n) performance
- **No Floating Point**: Maintains integer precision throughout
- **Optimized**: Critical path optimization for performance

### 3. **Result Handling**
- **Accumulation**: Boss collects all results before sorting
- **Memory Efficient**: Workers don't hold onto results after sending
- **Ordered Output**: Final results sorted by starting point

## Fault Tolerance

- **Actor Supervision**: Built into Gleam's OTP actor system
- **Message Reliability**: Process.call ensures reliable boss-worker communication
- **Timeout Protection**: 60-second timeout prevents infinite blocking

This architecture achieves excellent parallelism while maintaining code clarity and leveraging Gleam's actor model strengths.

