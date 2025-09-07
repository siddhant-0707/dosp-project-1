import gleam/list
import gleam/int
import gleam/erlang/process
import gleam/option
import gleam/otp/actor
import worker
import sysinfo

pub type BossMsg {
  Solve(reply_to: process.Subject(BossReply), n: Int, k: Int, work_unit: Int)
  WorkerDone(result: worker.WorkerResult)
  Stop
}

pub type BossReply {
  Solved(starts: List(Int))
}

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

type State {
  State(
    self_subject: process.Subject(BossMsg),
    workers: List(process.Subject(worker.ComputeMessage)),
    worker_results_subject: process.Subject(worker.WorkerResult),
    pending: Int,
    reply_to: option.Option(process.Subject(BossReply)),
    acc: List(Int),
    k_current: Int,
  )
}

fn init_state(
  self: process.Subject(BossMsg),
  worker_results_subject: process.Subject(worker.WorkerResult),
) -> State {
  let num = sysinfo.schedulers_online()
  let worker_count = case num > 0 {
    True -> num
    False -> 1
  }
  let workers = spawn_workers(worker_count, [])
  State(self, workers, worker_results_subject, 0, option.None, [], 0)
}

fn spawn_workers(n: Int, acc: List(process.Subject(worker.ComputeMessage))) -> List(process.Subject(worker.ComputeMessage)) {
  case n <= 0 {
    True -> acc
    False -> {
      let assert Ok(actor.Started(pid: _, data: subject)) = worker.start()
      spawn_workers(n - 1, [subject, ..acc])
    }
  }
}

fn handle(state: State, msg: BossMsg) -> actor.Next(State, BossMsg) {
  case msg {
    Stop -> actor.stop()

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

    WorkerDone(worker.Done(_, starts)) -> {
      let new_pending = state.pending - 1
      let new_acc = list.append(state.acc, starts)
      case new_pending == 0 {
        True -> {
          case state.reply_to {
            option.Some(r) -> process.send(r, Solved(list.sort(new_acc, by: int.compare)))
            option.None -> Nil
          }
          actor.continue(
            State(..state, pending: 0, acc: [], reply_to: option.None, k_current: 0),
          )
        }
        False -> actor.continue(State(..state, pending: new_pending, acc: new_acc))
      }
    }
  }
}

fn make_ranges(n: Int, size: Int) -> List(#(Int, Int)) {
  make_ranges_loop(1, n, size, [])
}

fn make_ranges_loop(start: Int, n: Int, size: Int, acc: List(#(Int, Int))) -> List(#(Int, Int)) {
  case start > n {
    True -> list.reverse(acc)
    False -> {
      let to = int.min(n, start + size - 1)
      make_ranges_loop(to + 1, n, size, [#(start, to), ..acc])
    }
  }
}

fn dispatch_jobs(state: State, ranges: List(#(Int, Int))) {
  dispatch_loop(state, ranges, 0)
}

fn dispatch_loop(state: State, ranges: List(#(Int, Int)), idx: Int) {
  case ranges {
    [] -> Nil
    [#(from, to), ..rest] -> {
      let w = pick_worker(state.workers, idx)
      case w {
        option.Some(subject) ->
          process.send(
            subject,
            worker.Compute(state.worker_results_subject, from, to, state.k_current),
          )
        option.None -> Nil
      }
      dispatch_loop(state, rest, idx + 1)
    }
  }
}

fn pick_worker(
  workers: List(process.Subject(worker.ComputeMessage)),
  idx: Int,
) -> option.Option(process.Subject(worker.ComputeMessage)) {
  let len = list.length(workers)
  case len == 0 {
    True -> option.None
    False -> nth(workers, idx % len)
  }
}

fn nth(
  list_: List(process.Subject(worker.ComputeMessage)),
  i: Int,
) -> option.Option(process.Subject(worker.ComputeMessage)) {
  nth_loop(list_, i, 0)
}

fn nth_loop(
  list_: List(process.Subject(worker.ComputeMessage)),
  i: Int,
  cur: Int,
) -> option.Option(process.Subject(worker.ComputeMessage)) {
  case list_ {
    [] -> option.None
    [first, ..rest] -> case cur == i {
      True -> option.Some(first)
      False -> nth_loop(rest, i, cur + 1)
    }
  }
}

pub fn solve(subject: process.Subject(BossMsg), n: Int, k: Int, work_unit: Int) -> List(Int) {
  let reply = process.call(subject, 60000, fn(reply) { Solve(reply, n, k, work_unit) })
  case reply {
    Solved(starts) -> starts
  }
}


