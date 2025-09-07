import argv
import gleam/erlang/atom
import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/order

// const epsilon = 0.0000000000000000000000000002

fn usage() {
  "usage: ./program <upper_bound>: Int <seq_length>: Int" |> io.println_error
}

pub fn schedulers_online() -> Int {
  system_info(atom.create("schedulers_online"))
}

@external(erlang, "erlang", "system_info")
fn system_info(a: atom.Atom) -> Int

pub type Config {
  Config(max_workers: Int, batch_size: Int)
}

pub fn main() {
  case
    argv.load().arguments
    |> list.map(int.parse)
  {
    [Ok(size), Ok(bound), Ok(length)] if length > 0 && bound > 0 -> {
      let res =
        cli_run(
          Config(max_workers: schedulers_online(), batch_size: size),
          bound,
          length,
        )
        |> result.map(fn(res) { res |> result.partition })
        |> result.map_error(fn(err) { #([], [err]) })
        |> result.unwrap_both

      res.0
      |> list.sort(int.compare)
      |> list.each(fn(n) { n |> int.to_string() |> io.println })
      res.1 |> list.each(fn(err) { { "Error: " <> err } |> io.println_error })
    }
    _ -> usage()
  }
}

type Supervisor {
  Supervisor(
    config: Config,
    parent: option.Option(Subject(List(Result(Int, String)))),
    workers: List(Subject(WorkerMessage)),
    bound: Int,
    length: Int,
    pending: Int,
    acc: List(Result(Int, String)),
  )
}

pub type ControlMessage {
  Start(
    parent: Subject(List(Result(Int, String))),
    self: Subject(ControlMessage),
  )
  Done(worker_id: Int, result: List(Result(Int, String)))
  Stop
}

pub type WorkerMessage {
  Work(reply_to: Subject(ControlMessage), from: Int, to: Int, length: Int)
  Terminate
}

fn cli_run(
  config: Config,
  bound: Int,
  length: Int,
) -> Result(List(Result(Int, String)), String) {
  Supervisor(
    config,
    option.None,
    list.range(1, config.max_workers)
      |> list.map(fn(id) { worker(id) }),
    bound,
    length,
    0,
    [],
  )
  |> actor.new()
  |> actor.on_message(aggregator)
  |> actor.start
  |> result.map(fn(agg) {
    agg.data |> actor.call(300_000, fn(sub) { Start(sub, agg.data) })
  })
  |> result.map_error(fn(err) {
    "Failed to start aggregator, " <> string.inspect(err)
  })
}

fn aggregator(
  sv: Supervisor,
  msg: ControlMessage,
) -> actor.Next(Supervisor, ControlMessage) {
  case msg {
    Stop -> actor.stop()

    Start(parent, self) -> {
      let chunks =
        { sv.bound + sv.config.batch_size - 1 } / sv.config.batch_size
      Supervisor(
        ..sv,
        parent: parent |> option.Some,
        pending: list.range(0, chunks - 1)
          |> list.map(fn(i) {
            #(
              1 + i * sv.config.batch_size,
              int.min({ i + 1 } * sv.config.batch_size, sv.bound),
            )
          })
          |> list.index_map(fn(e, idx) {
            {
              let assert Ok(w) =
                sv.workers
                |> list.drop(idx % sv.config.max_workers)
                |> list.first
              w
            }
            |> process.send(Work(self, e.0, e.1, sv.length))
          })
          |> list.length,
      )
      |> actor.continue
    }

    Done(_, res) -> {
      let new_acc = list.append(sv.acc, res)
      case sv.pending <= 1 {
        True -> {
          sv.parent
          |> option.map(fn(parent) { parent |> process.send(new_acc) })
          actor.stop()
        }
        False ->
          Supervisor(..sv, pending: sv.pending - 1, acc: new_acc)
          |> actor.continue
      }
    }
  }
}

fn worker(id: Int) -> Subject(WorkerMessage) {
  let assert Ok(started) =
    actor.new(id)
    |> actor.on_message(handle)
    |> actor.start

  started.data
}

fn handle(id: Int, msg: WorkerMessage) -> actor.Next(Int, WorkerMessage) {
  case msg {
    Terminate -> actor.stop()

    Work(parent, from, to, length) -> {
      parent
      |> actor.send(Done(
        id,
        accumulate_compute(from, to, length, [])
          |> list.map(fn(res) {
            res
            |> result.map_error(fn(e) {
              "worker " <> int.to_string(id) <> ", " <> e
            })
          }),
      ))
      actor.continue(id)
    }
  }
}

fn accumulate_compute(
  from: Int,
  to: Int,
  length: Int,
  acc: List(Result(Int, String)),
) -> List(Result(Int, String)) {
  case from > to {
    True -> acc
    False ->
      case compute(from, length) {
        Ok(True) ->
          accumulate_compute(from + 1, to, length, list.append(acc, [Ok(from)]))
        Ok(False) -> accumulate_compute(from + 1, to, length, acc)
        Error(err) ->
          accumulate_compute(
            from + 1,
            to,
            length,
            list.append(acc, [
              Error("seq_start " <> int.to_string(from) <> ", " <> err),
            ]),
          )
      }
  }
}

fn compute(start: Int, length: Int) -> Result(Bool, String) {
  { sum_of_squares(start + length - 1) -. sum_of_squares(start - 1) }
  |> float.square_root
  |> result.map(fn(x) {
    // float.loosely_equals(x, int.to_float(float.truncate(x)), epsilon)
    case float.compare(x, int.to_float(float.truncate(x))) {
      order.Eq -> True
      _ -> False
    }
  })
  |> result.map_error(fn(_) { "Failed to compute square root" })
}

fn sum_of_squares(x: Int) -> Float {
  int.to_float(x * { x + 1 } * { { 2 * x } + 1 }) /. 6.0
}
