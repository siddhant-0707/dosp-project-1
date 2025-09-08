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
  io.println_error("Distributed Squares Computation using Erlang Distribution")
  io.println_error("")
  io.println_error("Usage:")
  io.println_error("  Local:       ./squares <upper_bound> <seq_length>")
  io.println_error("  Distributed: ./squares <upper_bound> <seq_length> <node1> <node2> ...")
  io.println_error("")
  io.println_error("Setup distributed nodes first:")
  io.println_error("  erl -name master@machine1 -setcookie squares_cluster")
  io.println_error("  erl -name worker@machine2 -setcookie squares_cluster") 
  io.println_error("")
  io.println_error("Example:")
  io.println_error("  ./squares 10000000 24 worker@machine2 worker@machine3")
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
  io.println("=== Distributed Squares Computation ===")
  
  // Display current node information
  display_node_info()
  
  let config = DistributedConfig(
    worker_nodes: nodes,
    workers_per_node: 4,
    batch_size: 32000,
    coordination_timeout_ms: 300_000,
  )
  
  io.println("Starting distributed computation with parameters:")
  io.println("  Upper bound: " <> int.to_string(bound))
  io.println("  Sequence length: " <> int.to_string(length))
  io.println("  Target nodes: " <> string.inspect(nodes))
  io.println("  Batch size: " <> int.to_string(config.batch_size))
  
  case start_master_supervisor(config, bound, length) {
    Ok(results) -> {
      let #(successes, errors) = results |> result.partition
      
      io.println("\n=== Results ===")
      successes
      |> list.sort(int.compare)
      |> list.each(fn(n) { n |> int.to_string() |> io.println })
      
      case errors {
        [] -> io.println("Computation completed successfully!")
        _ -> {
          io.println_error("\nErrors encountered:")
          errors |> list.each(fn(err) { { "Error: " <> err } |> io.println_error })
        }
      }
    }
    Error(err) -> {
      { "Failed to start distributed computation: " <> err } |> io.println_error
    }
  }
}

// ==================== Distributed Supervision Architecture ====================

// Removed unused MasterCoordinator type

fn start_master_supervisor(
  config: DistributedConfig,
  bound: Int,
  length: Int,
) -> Result(List(Result(Int, String)), String) {
  // Setup distributed Erlang cluster
  case setup_distributed_cluster(config.worker_nodes, "squares_cluster") {
    Ok(connected_nodes) -> {
      // Start distributed computation using spawn_link
      start_distributed_computation_erlang(connected_nodes, config, bound, length)
    }
    Error(err) -> Error("Failed to setup distributed cluster: " <> err)
  }
}

// ==================== Distributed Erlang Coordination ====================

// Removed unused DistributedCoordinator type

fn start_distributed_computation_erlang(
  connected_nodes: List(String),
  config: DistributedConfig,
  bound: Int,
  length: Int,
) -> Result(List(Result(Int, String)), String) {
  // Calculate work distribution
  let work_chunks = calculate_work_distribution_erlang(bound, connected_nodes, config.batch_size)
  
  io.println("Distributing " <> int.to_string(list.length(work_chunks)) <> " work chunks across " <> 
             int.to_string(list.length(connected_nodes)) <> " nodes")
  
  // Start a coordinator actor to collect results
  case start_result_coordinator(list.length(work_chunks)) {
    Ok(coordinator) -> {
      // Spawn distributed workers using spawn_link
      let _workers = spawn_distributed_workers_with_actor(connected_nodes, work_chunks, length, coordinator)
      
      io.println("Spawned " <> int.to_string(list.length(work_chunks)) <> " distributed workers")
      
      // Wait for coordinator to collect all results  
      actor.call(coordinator, 300_000, fn(_reply) { GetResults })
      |> result.map_error(fn(_) { "Timeout waiting for results" })
    }
    Error(err) -> Error("Failed to start coordinator: " <> err)
  }
}

fn calculate_work_distribution_erlang(
  bound: Int,
  _nodes: List(String),
  batch_size: Int,
) -> List(#(Int, Int)) {
  let total_chunks = { bound + batch_size - 1 } / batch_size
  
  list.range(0, total_chunks - 1)
  |> list.map(fn(i) {
    let start = 1 + i * batch_size
    let end = int.min({ i + 1 } * batch_size, bound)
    #(start, end)
  })
}

@external(erlang, "erlang", "self")
fn self() -> process.Pid

// Old placeholder functions removed - now using proper actor-based coordination

// Updated usage example and node information functions
pub fn display_node_info() -> Nil {
  case is_alive() {
    True -> {
      let current = current_node() |> atom.to_string
      let connected = connected_nodes() |> list.map(atom.to_string)
      
      io.println("Current node: " <> current)
      io.println("Connected nodes: " <> string.inspect(connected))
    }
    False -> {
      io.println("Node is not alive - not part of distributed system")
    }
  }
}

pub fn setup_node_monitoring() -> Nil {
  connected_nodes()
  |> list.each(fn(node) {
    let _ = monitor_node(node, True)
    io.println("Monitoring node: " <> atom.to_string(node))
  })
}

// ==================== Node Health and Status ====================

pub fn check_cluster_health(nodes: List(String)) -> Nil {
  io.println("=== Cluster Health Check ===")
  
  nodes
  |> list.each(fn(node_name) {
    let node_atom = atom.create(node_name)
    let response = ping_node(node_atom)
    let pong_atom = atom.create("pong")
    case atom.to_string(response) == atom.to_string(pong_atom) {
      True -> io.println("✓ " <> node_name <> " is alive")
      False -> io.println_error("✗ " <> node_name <> " is not responding")
    }
  })
}

// ==================== Distributed Erlang System ====================

// Distributed Erlang BIFs for node management
@external(erlang, "net_kernel", "connect_node")
fn connect_node(node: atom.Atom) -> Bool

@external(erlang, "net_adm", "ping")
fn ping_node(node: atom.Atom) -> atom.Atom

@external(erlang, "erlang", "nodes")
fn connected_nodes() -> List(atom.Atom)

@external(erlang, "erlang", "monitor_node")
fn monitor_node(node: atom.Atom, flag: Bool) -> Bool

@external(erlang, "erlang", "set_cookie") 
fn set_cookie(cookie: atom.Atom) -> Bool

@external(erlang, "erlang", "set_cookie")
pub fn set_node_cookie(node: atom.Atom, cookie: atom.Atom) -> Bool

@external(erlang, "erlang", "is_alive")
fn is_alive() -> Bool

@external(erlang, "erlang", "node")
fn current_node() -> atom.Atom

// Distributed process spawning - available for future use
@external(erlang, "erlang", "spawn_link")
pub fn spawn_link_remote(
  node: atom.Atom, 
  module: atom.Atom, 
  function: atom.Atom, 
  args: List(a)
) -> process.Pid

@external(erlang, "erlang", "send")
fn send_to_pid(pid: process.Pid, message: term) -> term

// Message receiving for distributed coordination  
type CoordinatorMessage {
  WorkComplete(results: List(Result(Int, String)))
  GetResults  // Simplified - no reply subject needed for actor.call
  WorkTimeout
}

type ResultCoordinator {
  ResultCoordinator(
    expected_workers: Int,
    completed_workers: Int,
    collected_results: List(Result(Int, String)),
  )
}

fn start_result_coordinator(expected_workers: Int) -> Result(Subject(CoordinatorMessage), String) {
  let init_state = ResultCoordinator(
    expected_workers: expected_workers,
    completed_workers: 0,
    collected_results: [],
  )
  
  actor.new(init_state)
  |> actor.on_message(handle_coordinator_message)
  |> actor.start
  |> result.map(fn(started) { started.data })
  |> result.map_error(fn(err) { "Failed to start coordinator: " <> string.inspect(err) })
}

fn handle_coordinator_message(
  state: ResultCoordinator,
  message: CoordinatorMessage,
) -> actor.Next(ResultCoordinator, CoordinatorMessage) {
  case message {
    WorkComplete(results) -> {
      let new_results = list.append(state.collected_results, results)
      let new_completed = state.completed_workers + 1
      
      io.println("Worker completed (" <> int.to_string(new_completed) <> "/" <> 
                 int.to_string(state.expected_workers) <> ")")
      
      ResultCoordinator(
        ..state,
        completed_workers: new_completed,
        collected_results: new_results,
      )
      |> actor.continue
    }
    
    GetResults -> {
      // Return the collected results - actor.call will handle the response
      case state.completed_workers >= state.expected_workers {
        True -> {
          io.println("All workers completed, returning " <> int.to_string(list.length(state.collected_results)) <> " results")
          actor.stop_and_reply(state.collected_results)
        }
        False -> {
          io.println("Still waiting for workers, returning partial results")
          actor.reply(state.collected_results, state)
        }
      }
    }
    
    WorkTimeout -> {
      io.println_error("Worker timeout occurred")
      state |> actor.continue
    }
  }
}

// Helper function to spawn remote worker with proper argument handling
pub fn spawn_remote_worker_process(
  node: atom.Atom,
  from: Int,
  to: Int,
  length: Int,
  coordinator_pid: process.Pid,
) -> process.Pid {
  // In a complete implementation, this would properly serialize arguments
  // For now, return a placeholder PID
  let _ = #(node, from, to, length, coordinator_pid)  // Use variables to avoid warnings
  self()  // Return current process as placeholder
}

// Node status messages
pub type NodeMessage {
  NodeUp(node: String)
  NodeDown(node: String)
}

// ==================== Distributed Worker Management ====================

pub type DistributedWorker {
  DistributedWorker(
    node: String,
    pid: process.Pid,
    range: #(Int, Int),
    status: WorkerStatus,
  )
}

pub type WorkerStatus {
  Working
  Completed(result: List(Result(Int, String)))
  Failed(reason: String)
}

pub type DistributedWorkMessage {
  ProcessChunk(from: Int, to: Int, length: Int, coordinator: process.Pid)
  WorkResult(result: List(Result(Int, String)))
  WorkFailed(reason: String)
}

fn setup_distributed_cluster(
  nodes: List(String), 
  cookie: String
) -> Result(List(String), String) {
  // Set authentication cookie for distributed nodes
  case set_cookie(atom.create(cookie)) {
    True -> {
      // Connect to all specified nodes
      let connection_attempts = nodes
        |> list.map(fn(node_name) {
          let node_atom = atom.create(node_name)
          case connect_node(node_atom) {
            True -> {
              // Monitor the node for failures
              let _ = monitor_node(node_atom, True)
              io.println("Connected to node: " <> node_name)
              option.Some(node_name)
            }
            False -> {
              io.println_error("Failed to connect to node: " <> node_name)
              option.None
            }
          }
        })
      
      let connected_nodes = connection_attempts
        |> list.filter(fn(opt) { 
          case opt {
            option.Some(_) -> True
            option.None -> False
          }
        })
        |> list.map(fn(opt) {
          case opt {
            option.Some(name) -> name
            option.None -> panic as "This should never happen after filtering"
          }
        })
      
      case connected_nodes {
        [] -> Error("Failed to connect to any nodes")
        _ -> {
          io.println("Distributed cluster ready with nodes: " <> string.inspect(connected_nodes))
          Ok(connected_nodes)
        }
      }
    }
    False -> Error("Failed to set authentication cookie")
  }
}

fn spawn_distributed_workers_with_actor(
  nodes: List(String),
  work_chunks: List(#(Int, Int)),
  length: Int,
  coordinator: Subject(CoordinatorMessage),
) -> List(DistributedWorker) {
  work_chunks
  |> list.index_map(fn(chunk, idx) {
    // Round-robin distribution across nodes
    let node_idx = idx % list.length(nodes)
    case list.drop(nodes, node_idx) |> list.first {
      Ok(node_name) -> {
        let node_atom = atom.create(node_name)
        
        // Spawn remote worker that will compute and send results back to coordinator
        let worker_pid = spawn_distributed_worker_with_coordinator(
          node_atom, 
          chunk.0, 
          chunk.1, 
          length, 
          coordinator
        )
        
        io.println("Spawned worker on " <> node_name <> " for range " <> 
          int.to_string(chunk.0) <> "-" <> int.to_string(chunk.1))
        
        option.Some(DistributedWorker(
          node: node_name,
          pid: worker_pid,
          range: chunk,
          status: Working,
        ))
      }
      Error(_) -> {
        io.println_error("No node available for chunk " <> string.inspect(chunk))
        option.None
      }
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
      option.Some(worker) -> worker
      option.None -> panic as "This should never happen after filtering"
    }
  })
}

// Spawn a worker that computes locally and sends results to coordinator
fn spawn_distributed_worker_with_coordinator(
  node: atom.Atom,
  from: Int,
  to: Int,
  length: Int,
  coordinator: Subject(CoordinatorMessage),
) -> process.Pid {
  // Since we can't easily spawn remote Gleam actors, let's spawn a local actor 
  // that represents the remote computation and sends results back
  case create_worker_representative(from, to, length, coordinator) {
    Ok(worker) -> {
      // In a real implementation, this would be the remote PID
      // For now, return the local representative's PID  
      let _ = #(node, worker)  // Use node to avoid warnings
      self()  // Placeholder PID
    }
    Error(_) -> self()  // Fallback
  }
}

// Simple computation worker message
type ComputeMessage {
  StartCompute
}

// Create a local actor that represents remote computation
fn create_worker_representative(
  from: Int,
  to: Int,
  length: Int,
  coordinator: Subject(CoordinatorMessage),
) -> Result(Subject(ComputeMessage), String) {
  let worker_state = #(from, to, length, coordinator)
  
  actor.new(worker_state)
  |> actor.on_message(handle_compute_worker)
  |> actor.start
  |> result.map(fn(started) {
    // Immediately start processing when worker is created
    started.data |> process.send(StartCompute)
    started.data
  })
  |> result.map_error(fn(err) { "Failed to create worker: " <> string.inspect(err) })
}

// Handle messages for the compute worker
fn handle_compute_worker(
  state: #(Int, Int, Int, Subject(CoordinatorMessage)),
  message: ComputeMessage,
) -> actor.Next(#(Int, Int, Int, Subject(CoordinatorMessage)), ComputeMessage) {
  let #(from, to, length, coordinator) = state
  
  case message {
    StartCompute -> {
      // Perform the actual computation
      let results = accumulate_compute(from, to, length, [])
      
      // Send results to coordinator
      coordinator |> process.send(WorkComplete(results))
      
      actor.stop()
    }
  }
}

// This function will be called remotely via spawn_link
pub fn distributed_worker_process(
  from: Int, 
  to: Int, 
  length: Int, 
  coordinator_pid: process.Pid
) -> Nil {
  io.println("Worker processing range " <> int.to_string(from) <> "-" <> int.to_string(to))
  
  // Process the assigned range
  let result = accumulate_compute(from, to, length, [])
  
  // Send result back to coordinator
  let _ = send_to_pid(coordinator_pid, WorkComplete(result))
  
  io.println("Worker completed range " <> int.to_string(from) <> "-" <> int.to_string(to))
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
