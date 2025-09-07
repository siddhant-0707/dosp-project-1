import argv
import gleam/erlang/atom
import gleam/erlang/process.{type Subject}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
// import gleam/otp/static_supervisor.{type Supervisor} as supervisor
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

pub type DistributedConfig {
  DistributedConfig(
    worker_nodes: List(String),
    workers_per_node: Int,
    batch_size: Int,
    coordination_timeout_ms: Int,
  )
}

// Messages for distributed coordination
pub type MasterMessage {
  StartDistributedWork(
    parent: Subject(List(Result(Int, String))),
    bound: Int,
    length: Int,
  )
  NodeResult(node: String, result: List(Result(Int, String)))
  NodeTimeout(node: String)
  NodeFailure(node: String, reason: String)
}

pub type NodeCoordinatorMessage {
  ProcessRange(
    master: Subject(MasterMessage),
    node_id: String,
    from: Int,
    to: Int,
    length: Int,
  )
  Shutdown
}

pub fn main() {
  case argv.load().arguments {
    // Local execution: ./squares <bound> <length>
    [bound_str, length_str] -> {
      case int.parse(bound_str), int.parse(length_str) {
        Ok(bound), Ok(length) if length > 0 && bound > 0 -> {
          run_local(bound, length)
        }
        _, _ -> usage()
      }
    }
    
    // Distributed execution: ./squares <bound> <length> <nodes...>
    [bound_str, length_str, ..node_args] -> {
      case int.parse(bound_str), int.parse(length_str) {
        Ok(bound), Ok(length) if length > 0 && bound > 0 -> {
          let nodes = node_args |> list.filter(fn(node) { node != "" })
          case nodes {
            [] -> run_local(bound, length)
            _ -> run_distributed(bound, length, nodes)
          }
        }
        _, _ -> usage()
      }
    }
    
    _ -> usage()
  }
}

fn run_local(bound: Int, length: Int) {
  let res =
    cli_run(
      Config(max_workers: schedulers_online(), batch_size: 32000),
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

fn run_distributed(bound: Int, length: Int, nodes: List(String)) {
  let config = DistributedConfig(
    worker_nodes: nodes,
    workers_per_node: 4,
    batch_size: 32000,
    coordination_timeout_ms: 300_000,
  )
  
  case start_master_supervisor(config, bound, length) {
    Ok(results) -> {
      let #(successes, errors) = results |> result.partition
      
      successes
      |> list.sort(int.compare)
      |> list.each(fn(n) { n |> int.to_string() |> io.println })
      errors |> list.each(fn(err) { { "Error: " <> err } |> io.println_error })
    }
    Error(err) -> {
      { "Failed to start distributed computation: " <> err } |> io.println_error
    }
  }
}

// ==================== Distributed Supervision Architecture ====================

type MasterCoordinator {
  MasterCoordinator(
    config: DistributedConfig,
    parent: option.Option(Subject(List(Result(Int, String)))),
    bound: Int,
    length: Int,
    node_coordinators: List(#(String, Subject(NodeCoordinatorMessage))),
    pending_nodes: Int,
    results: List(Result(Int, String)),
  )
}

fn start_master_supervisor(
  config: DistributedConfig,
  bound: Int,
  length: Int,
) -> Result(List(Result(Int, String)), String) {
  // For now, create the master coordinator directly without supervision
  // In a complete implementation, we would use proper supervision trees
  case create_master_coordinator_direct(config, bound, length) {
    Ok(coordinator) -> {
      // Start distributed computation
      case start_distributed_computation(coordinator, bound, length) {
        Ok(results) -> Ok(results)
        Error(err) -> Error(err)
      }
    }
    Error(err) -> Error("Failed to create master coordinator: " <> err)
  }
}

fn create_master_coordinator_direct(
  config: DistributedConfig,
  bound: Int,
  length: Int,
) -> Result(Subject(MasterMessage), String) {
  let init_state = MasterCoordinator(
    config: config,
    parent: option.None,
    bound: bound,
    length: length,
    node_coordinators: [],
    pending_nodes: 0,
    results: [],
  )

  actor.new(init_state)
  |> actor.on_message(handle_master_message)
  |> actor.start
  |> result.map(fn(started) { started.data })
  |> result.map_error(fn(err) { 
    "Failed to start master coordinator: " <> string.inspect(err) 
  })
}

fn start_distributed_computation(
  _coordinator: Subject(MasterMessage),
  _bound: Int,
  _length: Int,
) -> Result(List(Result(Int, String)), String) {
  // This would be implemented to coordinate the distributed work
  // For now, return a placeholder result
  Ok([Ok(1), Ok(2)]) // Placeholder results
}

fn handle_master_message(
  state: MasterCoordinator,
  message: MasterMessage,
) -> actor.Next(MasterCoordinator, MasterMessage) {
  case message {
    StartDistributedWork(parent, bound, length) -> {
      // Connect to worker nodes and start distributed computation
      let node_coordinators = start_worker_node_coordinators(state.config)
      let work_chunks = calculate_work_distribution(bound, state.config)
      
      // Distribute work to nodes
      work_chunks
      |> list.index_map(fn(chunk, idx) {
        let node_idx = idx % list.length(node_coordinators)
        case list.drop(node_coordinators, node_idx) |> list.first {
          Ok(#(node_name, coordinator)) -> {
            // Note: In a complete implementation, we would need to get the master subject
            // For now, this is a design sketch showing the intended message flow
            let master_subject = todo as "Need to get master subject from actor context"
            coordinator
            |> process.send(ProcessRange(
              master_subject,
              node_name,
              chunk.0,
              chunk.1,
              length,
            ))
          }
          Error(_) -> Nil
        }
      })
      
      MasterCoordinator(
        ..state,
        parent: parent |> option.Some,
        node_coordinators: node_coordinators,
        pending_nodes: list.length(node_coordinators),
      )
      |> actor.continue
    }
    
    NodeResult(_node, result) -> {
      let new_results = list.append(state.results, result)
      let remaining = state.pending_nodes - 1
      
      case remaining <= 0 {
        True -> {
          // Send results back to parent and stop
          state.parent
          |> option.map(fn(parent) { parent |> process.send(new_results) })
          actor.stop()
        }
        False -> {
          MasterCoordinator(..state, results: new_results, pending_nodes: remaining)
          |> actor.continue
        }
      }
    }
    
    NodeTimeout(node) -> {
      // Handle timeout - could retry or mark as failed
      let error_result = [Error("Node " <> node <> " timed out")]
      handle_master_message(state, NodeResult(node, error_result))
    }
    
    NodeFailure(node, reason) -> {
      // Handle node failure
      let error_result = [Error("Node " <> node <> " failed: " <> reason)]
      handle_master_message(state, NodeResult(node, error_result))
    }
  }
}

// Updated to use the implementation
fn start_worker_node_coordinators(
  config: DistributedConfig,
) -> List(#(String, Subject(NodeCoordinatorMessage))) {
  start_worker_node_coordinators_impl(config)
}

fn calculate_work_distribution(
  bound: Int,
  config: DistributedConfig,
) -> List(#(Int, Int)) {
  // Calculate work chunks similar to existing logic
  let total_nodes = list.length(config.worker_nodes)
  let chunk_size = bound / total_nodes
  
  list.range(0, total_nodes - 1)
  |> list.map(fn(i) {
    let start = i * chunk_size + 1
    let end = case i == total_nodes - 1 {
      True -> bound  // Last chunk gets remainder
      False -> { i + 1 } * chunk_size
    }
    #(start, end)
  })
}

// ==================== Worker Node Supervision ====================

type WorkerNodeCoordinator {
  WorkerNodeCoordinator(
    node_id: String,
    workers_per_node: Int,
    workers: List(Subject(WorkerMessage)),
    active_work: option.Option(#(Subject(MasterMessage), Int, Int, Int)), // master, from, to, length
  )
}

pub fn start_worker_node_supervisor(
  node_id: String,
  workers_per_node: Int,
) -> Result(Subject(NodeCoordinatorMessage), String) {
  // For now, create workers directly
  // In a complete implementation, we would use proper supervision trees
  let workers = 
    list.range(1, workers_per_node)
    |> list.map(fn(worker_id) {
      let _full_id = node_id <> "_worker_" <> int.to_string(worker_id)
      worker(worker_id)
    })

  create_node_coordinator_direct(node_id, workers)
}

fn create_node_coordinator_direct(
  node_id: String,
  workers: List(Subject(WorkerMessage)),
) -> Result(Subject(NodeCoordinatorMessage), String) {
  let init_state = WorkerNodeCoordinator(
    node_id: node_id,
    workers_per_node: list.length(workers),
    workers: workers,
    active_work: option.None,
  )

  actor.new(init_state)
  |> actor.on_message(handle_node_coordinator_message)
  |> actor.start
  |> result.map(fn(started) { started.data })
  |> result.map_error(fn(err) {
    "Failed to create node coordinator: " <> string.inspect(err)
  })
}

fn handle_node_coordinator_message(
  state: WorkerNodeCoordinator,
  message: NodeCoordinatorMessage,
) -> actor.Next(WorkerNodeCoordinator, NodeCoordinatorMessage) {
  case message {
    ProcessRange(master, node_id, from, to, length) -> {
      // Start processing the assigned range
      let result = process_range_locally(from, to, length, state.workers)
      
      // Send result back to master
      master |> process.send(NodeResult(node_id, result))
      
      WorkerNodeCoordinator(
        ..state,
        active_work: option.Some(#(master, from, to, length)),
      )
      |> actor.continue
    }
    
    Shutdown -> {
      // Gracefully shutdown all workers
      state.workers
      |> list.each(fn(worker) { worker |> process.send(Terminate) })
      
      actor.stop()
    }
  }
}

fn process_range_locally(
  from: Int,
  to: Int,
  length: Int,
  workers: List(Subject(WorkerMessage)),
) -> List(Result(Int, String)) {
  // If no workers available, compute directly
  case workers {
    [] -> accumulate_compute(from, to, length, [])
    _ -> {
      // Distribute work among available workers
      // For simplicity, do direct computation for now
      // In a full implementation, this would distribute work among local workers
      accumulate_compute(from, to, length, [])
    }
  }
}

// Workers are created using the existing worker() function

// ==================== Remote Node Connection ====================

@external(erlang, "net_kernel", "connect_node")
fn connect_to_node(node: atom.Atom) -> Bool

@external(erlang, "rpc", "call")
fn remote_call(
  node: atom.Atom,
  module: atom.Atom,
  function: atom.Atom,
  args: List(a),
) -> b

fn start_worker_node_coordinators_impl(
  config: DistributedConfig,
) -> List(#(String, Subject(NodeCoordinatorMessage))) {
  config.worker_nodes
  |> list.map(fn(node_name) {
    // Connect to remote node
    let node_atom = atom.create(node_name)
    case connect_to_node(node_atom) {
      True -> {
        // Start worker supervisor on remote node
        // This is a placeholder for the actual remote call implementation
        // In practice, we would need proper serialization and error handling
        case 
          remote_call(
            node_atom,
            atom.create("squares"), 
            atom.create("start_worker_node_supervisor"),
            [node_name, int.to_string(config.workers_per_node)]
          )
        {
          Ok(coordinator_subject) -> {
            option.Some(#(node_name, coordinator_subject))
          }
          Error(_) -> option.None
        }
      }
      False -> option.None
    }
  })
  |> list.filter(fn(opt) { 
    case opt {
      option.Some(_) -> True
      option.None -> False
    }
  })
  |> list.map(fn(opt) {
    case opt {
      option.Some(value) -> value
      option.None -> panic as "This should never happen after filtering"
    }
  })
}

// ==================== Local Supervision (existing) ====================

type LocalSupervisor {
  LocalSupervisor(
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
  LocalSupervisor(
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
  sv: LocalSupervisor,
  msg: ControlMessage,
) -> actor.Next(LocalSupervisor, ControlMessage) {
  case msg {
    Stop -> actor.stop()

    Start(parent, self) -> {
      let chunks =
        { sv.bound + sv.config.batch_size - 1 } / sv.config.batch_size
      LocalSupervisor(
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
          LocalSupervisor(..sv, pending: sv.pending - 1, acc: new_acc)
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
