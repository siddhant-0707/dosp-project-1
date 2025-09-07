import gleam/io
// import gleam/string
import gleam/result
import gleam/int
import argv
import boss
import gleam/otp/actor
// import gleam/erlang/process
import gleam/list

pub fn main() -> Nil {
  let args = argv.load().arguments
  case args {
    [n_str, k_str] -> {
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

fn usage() -> Nil {
  io.println("Usage: squares N k")
}

fn run(n: Int, k: Int) -> Nil {
  let assert Ok(actor.Started(_, data: subject)) = boss.start()
  // let work_unit = pick_work_unit(k)
  let work_unit = pick_work_unit(2000)
  let starts = boss.solve(subject, n, k, work_unit)
  print_starts(starts)
}

fn pick_work_unit(k: Int) -> Int {
  // heuristic: larger k means more computation per start, use larger chunks
  case k {
    _ if k >= 1000 -> 20000
    _ if k >= 100 -> 10000
    _ if k >= 20 -> 5000
    _ -> 2000
  }
}

fn print_starts(starts: List(Int)) -> Nil {
  starts
  |> list.each(fn(s) { io.println(int.to_string(s)) })
}
