import gleam/list
import gleam/erlang/process
import gleam/otp/actor

pub type ComputeMessage {
  Compute(reply_to: process.Subject(WorkerResult), from: Int, to: Int, k: Int)
  Stop
}

pub type WorkerResult {
  Done(from_worker: process.Subject(ComputeMessage), starts: List(Int))
}

pub fn start() -> actor.StartResult(process.Subject(ComputeMessage)) {
  let builder =
    actor.new_with_initialiser(1000, fn(subject) {
      actor.initialised(subject) |> actor.returning(subject) |> Ok
    })
    |> actor.on_message(handle_message)

  actor.start(builder)
}

fn handle_message(
  self_subject: process.Subject(ComputeMessage),
  message: ComputeMessage,
) -> actor.Next(process.Subject(ComputeMessage), ComputeMessage) {
  case message {
    Stop -> actor.stop()

    Compute(reply_to, from, to, k) -> {
      let starts = find_starts_in_range(from, to, k)
      process.send(reply_to, Done(self_subject, starts))
      actor.continue(self_subject)
    }
  }
}

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

fn is_perfect_square(n: Int) -> Bool {
  case n < 0 {
    True -> False
    False -> {
      let r = isqrt(n)
      r * r == n
    }
  }
}

fn isqrt(n: Int) -> Int {
  // Integer square root via binary search
  isqrt_search(n, 0, n)
}

fn isqrt_search(n: Int, low: Int, high: Int) -> Int {
  case low > high {
    True -> high
    False -> {
      let mid = {low + high} / 2
      let sq = mid * mid
      case sq == n {
        True -> mid
        False -> case sq < n {
          True -> isqrt_search(n, mid + 1, high)
          False -> isqrt_search(n, low, mid - 1)
        }
      }
    }
  }
}


