// project3.gleam - Chord P2P Implementation (Enhanced)
// Adds: dynamic join, periodic stabilize/fix_fingers/check_predecessor,
// and key transfer on join. Compatible with base structure you shared.

import argv
import gleam/dict.{type Dict}
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result

// =============================================================================
// CONFIGURATION
// =============================================================================

const m = 20

// ring bits (2^m space)

const stabilize_ms = 100

const fix_fingers_ms = 100

const check_pred_ms = 250

// =============================================================================
// TYPES
// =============================================================================

pub type NodeId =
  Int

pub type Key =
  Int

pub type RequestId =
  Int

// --- Messages that nodes can receive ---
pub type NodeMessage {
  // Base API
  FindSuccessor(key: Key, reply_to: NodeId, request_id: RequestId, hops: Int)
  FoundSuccessor(successor: NodeId, request_id: RequestId, hops: Int)
  Store(key: Key, value: String)
  Lookup(key: Key, reply_to: NodeId, request_id: RequestId, hops: Int)
  LookupResponse(
    found: Bool,
    value: Option(String),
    request_id: RequestId,
    hops: Int,
  )
  SetSelf(
    self: process.Subject(NodeMessage),
    registry: Dict(NodeId, process.Subject(NodeMessage)),
  )
  SetPredecessor(pred: NodeId)
  Shutdown

  // Requests pacing
  StartRequests(total: Int, coordinator: process.Subject(CoordMessage))
  NextRequest(remaining: Int, coordinator: process.Subject(CoordMessage))

  // Dynamic join (new node contacting an existing node to join the ring)
  Join(new_node_id: NodeId, new_node_subj: process.Subject(NodeMessage))
  // Neighbor updates
  SetSuccessor(succ: NodeId)
  NotifyPredecessor(candidate_pred: NodeId)

  // Demo join message (for testing)
  JoinDemo(registry: Dict(NodeId, process.Subject(NodeMessage)))

  // Key transfer protocol (on join)
  RequestKeysForJoin(new_id: NodeId, new_pred: NodeId)
  TransferKeys(pairs: List(#(Key, String)))

  // Periodic maintenance
  StabilizeTick
  FixFingersTick
  CheckPredecessorTick
}

// Coordinator messages
pub type CoordMessage {
  ReportHops(hops: Int)
  RequestComplete
}

// Node state
pub type NodeState {
  NodeState(
    id: NodeId,
    successor: NodeId,
    predecessor: Option(NodeId),
    finger_table: List(NodeId),
    storage: Dict(Key, String),
    self: Option(process.Subject(NodeMessage)),
    node_registry: Dict(NodeId, process.Subject(NodeMessage)),
    pending_requests: Dict(RequestId, process.Subject(CoordMessage)),
    next_request_id: Int,
  )
}

// Coordinator state
pub type CoordState {
  CoordState(total_requests: Int, completed_requests: Int, total_hops: Int)
}

// =============================================================================
// UTILITIES
// =============================================================================

fn pow(base: Int, exp: Int) -> Int {
  case exp {
    0 -> 1
    _ -> base * pow(base, exp - 1)
  }
}

fn mod_ring(x: Int) -> Int {
  int.modulo(x, pow(2, m)) |> result.unwrap(0)
}

fn in_range(key: Key, start: Key, end_val: Key) -> Bool {
  case start < end_val {
    True -> key > start && key <= end_val
    False -> key > start || key <= end_val
  }
}

fn sort_nodes(nodes: List(NodeId)) -> List(NodeId) {
  list.sort(nodes, int.compare)
}

fn find_responsible_node(key: Key, nodes: List(NodeId)) -> NodeId {
  let sorted = sort_nodes(nodes)
  case list.find(sorted, fn(n) { n >= key }) {
    Ok(node) -> node
    Error(_) ->
      case list.first(sorted) {
        Ok(first) -> first
        Error(_) -> 0
      }
  }
}

fn build_finger_table(node_id: NodeId, all_nodes: List(NodeId)) -> List(NodeId) {
  let sorted = sort_nodes(all_nodes)
  list.range(0, m)
  |> list.map(fn(i) {
    let start = mod_ring(node_id + pow(2, i))
    find_responsible_node(start, sorted)
  })
}

fn closest_preceding_finger(
  key: Key,
  node_id: NodeId,
  finger_table: List(NodeId),
) -> Option(NodeId) {
  finger_table
  |> list.reverse()
  |> list.find(fn(finger) {
    finger != node_id && in_range(finger, node_id, key)
  })
  |> option.from_result()
}

fn generate_key(node_id: NodeId, request_num: Int) -> Key {
  let hash_base = node_id * 7919 + request_num * 104_729
  mod_ring(hash_base)
}

fn generate_node_ids(count: Int) -> List(NodeId) {
  let ring_size = pow(2, m)
  list.range(0, count - 1)
  |> list.map(fn(i) { i * ring_size / count })
}

// =============================================================================
// NODE ACTOR IMPLEMENTATION
// =============================================================================

fn start_node(
  id: NodeId,
  successor: NodeId,
  all_nodes: List(NodeId),
) -> Result(process.Subject(NodeMessage), actor.StartError) {
  let finger_table = build_finger_table(id, all_nodes)
  let initial_state =
    NodeState(
      id: id,
      successor: successor,
      predecessor: None,
      finger_table: finger_table,
      storage: dict.new(),
      self: None,
      node_registry: dict.new(),
      pending_requests: dict.new(),
      next_request_id: 0,
    )
  case
    actor.new(initial_state)
    |> actor.on_message(handle_message)
    |> actor.start()
  {
    Ok(started) -> Ok(started.data)
    Error(e) -> Error(e)
  }
}

fn schedule_ticks(self: process.Subject(NodeMessage)) -> Nil {
  process.send_after(self, stabilize_ms, StabilizeTick)
  process.send_after(self, fix_fingers_ms, FixFingersTick)
  let _ = process.send_after(self, check_pred_ms, CheckPredecessorTick)
  Nil
}

fn handle_message(
  state: NodeState,
  message: NodeMessage,
) -> actor.Next(NodeState, NodeMessage) {
  case message {
    SetSelf(self, registry) -> {
      // Start periodic maintenance when node becomes fully configured
      schedule_ticks(self)
      actor.continue(
        NodeState(..state, self: Some(self), node_registry: registry),
      )
    }

    SetPredecessor(pred) ->
      actor.continue(NodeState(..state, predecessor: Some(pred)))

    SetSuccessor(succ) -> actor.continue(NodeState(..state, successor: succ))

    Store(key, value) -> {
      let new_storage = dict.insert(state.storage, key, value)
      actor.continue(NodeState(..state, storage: new_storage))
    }

    Lookup(key, reply_to, request_id, hops) ->
      handle_lookup(key, reply_to, request_id, hops, state)

    LookupResponse(found, value, request_id, hops) ->
      handle_lookup_response(found, value, request_id, hops, state)

    FindSuccessor(key, reply_to, request_id, hops) ->
      handle_find_successor(key, reply_to, request_id, hops, state)

    FoundSuccessor(_successor, _request_id, _hops) -> actor.continue(state)

    StartRequests(total, coordinator) ->
      handle_start_requests(total, coordinator, state)

    NextRequest(remaining, coordinator) ->
      handle_next_request(remaining, coordinator, state)

    // ------- Dynamic Join -------
    Join(new_id, new_subj) -> {
      // We help the new node join by directing it to its successor and updating neighbors.
      let all_nodes = dict.keys(state.node_registry)
      let succ = find_responsible_node(new_id, all_nodes)

      // Tell new node its successor
      process.send(new_subj, SetSuccessor(succ))

      // Determine predecessor of successor in the sorted list
      let sorted = sort_nodes(all_nodes)
      // Find predecessor by looking through the sorted list
      let idx =
        list.index_map(sorted, fn(i, n) {
          case n == succ {
            True -> i
            False -> -1
          }
        })
        |> list.fold(0, fn(acc, i) {
          case i >= 0 {
            True -> i
            False -> acc
          }
        })

      let pred_idx = case idx {
        0 -> list.length(sorted) - 1
        _ -> idx - 1
      }

      let pred = case list.drop(sorted, pred_idx) {
        [p, ..] -> p
        [] -> succ
      }

      // Inform successor to adopt new predecessor if appropriate
      case dict.get(state.node_registry, succ) {
        Ok(succ_subj) -> process.send(succ_subj, NotifyPredecessor(new_id))
        Error(_) -> Nil
      }
      // Inform predecessor to set its successor to the new node (best-effort)
      case dict.get(state.node_registry, pred) {
        Ok(pred_subj) -> process.send(pred_subj, SetSuccessor(new_id))
        Error(_) -> Nil
      }

      // Ask successor to transfer keys that now belong to the new node
      case dict.get(state.node_registry, succ) {
        Ok(succ_subj) ->
          process.send(succ_subj, RequestKeysForJoin(new_id, pred))
        Error(_) -> Nil
      }

      // New node should also know about registry and predecessor
      process.send(new_subj, SetSelf(new_subj, state.node_registry))
      process.send(new_subj, SetPredecessor(pred))

      actor.continue(state)
    }

    NotifyPredecessor(candidate_pred) -> {
      // If we don't have a predecessor or candidate lies in (pred, self], adopt it
      let adopt = case state.predecessor {
        Some(p) -> in_range(candidate_pred, p, state.id)
        None -> True
      }
      case adopt {
        True ->
          actor.continue(NodeState(..state, predecessor: Some(candidate_pred)))
        False -> actor.continue(state)
      }
    }

    // Key transfer: successor computes keys in (new_pred, new_id] and sends them
    RequestKeysForJoin(new_id, new_pred) -> {
      let elig = keys_for_new_owner(state.storage, new_pred, new_id)
      let new_storage = remove_keys(state.storage, elig)
      let pairs = dict.to_list(elig)
      // Send to new node
      case dict.get(state.node_registry, new_id) {
        Ok(new_subj) -> process.send(new_subj, TransferKeys(pairs))
        Error(_) -> Nil
      }
      actor.continue(NodeState(..state, storage: new_storage))
    }

    TransferKeys(pairs) -> {
      let updated =
        list.fold(pairs, state.storage, fn(acc, pair) {
          let #(k, v) = pair
          dict.insert(acc, k, v)
        })
      actor.continue(NodeState(..state, storage: updated))
    }

    // Periodic maintenance (re-schedule after handling)
    StabilizeTick -> {
      // Ask successor to consider us as predecessor
      case dict.get(state.node_registry, state.successor) {
        Ok(succ_subj) -> process.send(succ_subj, NotifyPredecessor(state.id))
        Error(_) -> Nil
      }
      case state.self {
        Some(self) -> {
          process.send_after(self, stabilize_ms, StabilizeTick)
          actor.continue(state)
        }
        None -> actor.continue(state)
      }
    }

    FixFingersTick -> {
      let all_nodes = dict.keys(state.node_registry)
      let ft = build_finger_table(state.id, all_nodes)
      case state.self {
        Some(self) -> {
          process.send_after(self, fix_fingers_ms, FixFingersTick)
          actor.continue(NodeState(..state, finger_table: ft))
        }
        None -> actor.continue(NodeState(..state, finger_table: ft))
      }
    }

    CheckPredecessorTick -> {
      // Best-effort: if predecessor not in registry, clear it
      let keep = case state.predecessor {
        Some(p) -> dict.has_key(state.node_registry, p)
        None -> True
      }
      let new_state = case keep {
        True -> state
        False -> NodeState(..state, predecessor: None)
      }
      case state.self {
        Some(self) -> {
          process.send_after(self, check_pred_ms, CheckPredecessorTick)
          actor.continue(new_state)
        }
        None -> actor.continue(new_state)
      }
    }

    Shutdown -> actor.stop()

    JoinDemo(registry) -> {
      // Pick a new ID roughly mid-way between two nodes to force key transfer
      let all_ids = dict.keys(registry)
      let sorted = sort_nodes(all_ids)
      let mid = case list.first(sorted) {
        Ok(fst) -> fst + pow(2, m) / 2
        Error(_) -> pow(2, m) / 3
      }
      let new_id = mod_ring(mid)
      let successor = find_responsible_node(new_id, sorted)
      let res = start_node(new_id, successor, list.append(sorted, [new_id]))
      case res {
        Ok(new_subj) -> {
          // Register new node with all existing nodes by re-broadcasting registry
          let new_registry = dict.insert(registry, new_id, new_subj)
          list.each(dict.values(new_registry), fn(s) {
            process.send(s, SetSelf(s, new_registry))
          })
          // Ask current node to help the new one join
          case state.self {
            Some(self) -> process.send(self, Join(new_id, new_subj))
            None -> Nil
          }
          actor.continue(state)
        }
        Error(_) -> actor.continue(state)
      }
    }
  }
}

fn keys_for_new_owner(
  storage: Dict(Key, String),
  pred: NodeId,
  new_id: NodeId,
) -> Dict(Key, String) {
  let pairs = dict.to_list(storage)
  let elig_list =
    list.filter(pairs, fn(pair) {
      let #(k, _v) = pair
      in_range(k, pred, new_id)
    })
  dict.from_list(elig_list)
}

fn remove_keys(
  storage: Dict(Key, String),
  to_remove: Dict(Key, String),
) -> Dict(Key, String) {
  let keys = dict.keys(to_remove)
  list.fold(keys, storage, fn(acc, k) { dict.delete(acc, k) })
}

fn handle_lookup(
  key: Key,
  reply_to: NodeId,
  request_id: RequestId,
  hops: Int,
  state: NodeState,
) -> actor.Next(NodeState, NodeMessage) {
  case dict.get(state.storage, key) {
    Ok(value) -> {
      send_to_node(
        state.node_registry,
        reply_to,
        LookupResponse(True, Some(value), request_id, hops),
      )
      actor.continue(state)
    }
    Error(_) -> {
      let responsible = case state.predecessor {
        Some(pred) -> in_range(key, pred, state.id)
        None -> False
      }
      case responsible {
        True -> {
          send_to_node(
            state.node_registry,
            reply_to,
            LookupResponse(False, None, request_id, hops),
          )
          actor.continue(state)
        }
        False -> {
          let next_hop = case
            closest_preceding_finger(key, state.id, state.finger_table)
          {
            Some(n) -> n
            None -> state.successor
          }
          send_to_node(
            state.node_registry,
            next_hop,
            Lookup(key, reply_to, request_id, hops + 1),
          )
          actor.continue(state)
        }
      }
    }
  }
}

fn handle_lookup_response(
  _found: Bool,
  _value: Option(String),
  request_id: RequestId,
  hops: Int,
  state: NodeState,
) -> actor.Next(NodeState, NodeMessage) {
  case dict.get(state.pending_requests, request_id) {
    Ok(coordinator) -> {
      process.send(coordinator, ReportHops(hops))
      process.send(coordinator, RequestComplete)
      let new_pending = dict.delete(state.pending_requests, request_id)
      actor.continue(NodeState(..state, pending_requests: new_pending))
    }
    Error(_) -> actor.continue(state)
  }
}

fn handle_find_successor(
  key: Key,
  reply_to: NodeId,
  request_id: RequestId,
  hops: Int,
  state: NodeState,
) -> actor.Next(NodeState, NodeMessage) {
  case in_range(key, state.id, state.successor) {
    True -> {
      send_to_node(
        state.node_registry,
        reply_to,
        FoundSuccessor(state.successor, request_id, hops),
      )
      actor.continue(state)
    }
    False -> {
      let next_hop = case
        closest_preceding_finger(key, state.id, state.finger_table)
      {
        Some(n) -> n
        None -> state.successor
      }
      send_to_node(
        state.node_registry,
        next_hop,
        FindSuccessor(key, reply_to, request_id, hops + 1),
      )
      actor.continue(state)
    }
  }
}

fn handle_start_requests(
  total: Int,
  coordinator: process.Subject(CoordMessage),
  state: NodeState,
) -> actor.Next(NodeState, NodeMessage) {
  case total > 0, state.self {
    True, Some(self) -> {
      process.send_after(
        self,
        500 + state.id % 500,
        NextRequest(total, coordinator),
      )

      actor.continue(state)
    }
    _, _ -> actor.continue(state)
  }
}

fn handle_next_request(
  remaining: Int,
  coordinator: process.Subject(CoordMessage),
  state: NodeState,
) -> actor.Next(NodeState, NodeMessage) {
  case remaining > 0 {
    False -> actor.continue(state)
    True -> {
      let request_num = state.next_request_id
      let key = generate_key(state.id, request_num)
      let request_id = state.id * 100_000 + state.next_request_id
      let new_pending =
        dict.insert(state.pending_requests, request_id, coordinator)
      case state.self {
        Some(self) -> {
          process.send(self, Lookup(key, state.id, request_id, 0))
          let new_state =
            NodeState(
              ..state,
              pending_requests: new_pending,
              next_request_id: state.next_request_id + 1,
            )
          case remaining > 1 {
            True -> {
              process.send_after(
                self,
                1000,
                NextRequest(remaining - 1, coordinator),
              )
              actor.continue(new_state)
            }
            False -> actor.continue(new_state)
          }
        }
        None -> actor.continue(state)
      }
    }
  }
}

fn send_to_node(
  registry: Dict(NodeId, process.Subject(NodeMessage)),
  node_id: NodeId,
  message: NodeMessage,
) -> Nil {
  case dict.get(registry, node_id) {
    Ok(subject) -> process.send(subject, message)
    Error(_) -> Nil
  }
}

// =============================================================================
// COORDINATOR + SIMULATION
// =============================================================================

fn init_coordinator(num_nodes: Int, num_requests: Int) -> CoordState {
  CoordState(
    total_requests: num_nodes * num_requests,
    completed_requests: 0,
    total_hops: 0,
  )
}

fn collect_results(
  coordinator: process.Subject(CoordMessage),
  state: CoordState,
) -> CoordState {
  case state.completed_requests >= state.total_requests {
    True -> state
    False -> {
      case process.receive(coordinator, 300_000) {
        Ok(ReportHops(hops)) -> {
          let new_state =
            CoordState(..state, total_hops: state.total_hops + hops)
          collect_results(coordinator, new_state)
        }
        Ok(RequestComplete) -> {
          let new_state =
            CoordState(
              ..state,
              completed_requests: state.completed_requests + 1,
            )

          // Calculate detailed progress percentage
          let progress_float =
            int.to_float(new_state.completed_requests)
            /. int.to_float(state.total_requests)
            *. 100.0

          let progress_int = float.truncate(progress_float)
          let prev_progress_float =
            int.to_float(state.completed_requests)
            /. int.to_float(state.total_requests)
            *. 100.0
          let prev_progress_int = float.truncate(prev_progress_float)

          // Print progress on every 1% change
          case progress_int > prev_progress_int {
            True -> {
              io.println(
                "Progress: "
                <> float.to_string(progress_float)
                <> "% ("
                <> int.to_string(new_state.completed_requests)
                <> "/"
                <> int.to_string(state.total_requests)
                <> " requests, avg hops: "
                <> float.to_string(
                  int.to_float(new_state.total_hops)
                  /. int.to_float(new_state.completed_requests),
                )
                <> ")",
              )
            }
            False -> Nil
          }

          collect_results(coordinator, new_state)
        }
        Error(_) -> {
          io.println("Timeout waiting for results")
          state
        }
      }
    }
  }
}

fn print_stats(state: CoordState) -> Nil {
  io.println("\n=== Results ===")
  io.println("Total requests: " <> int.to_string(state.total_requests))
  io.println("Completed: " <> int.to_string(state.completed_requests))
  io.println("Total hops: " <> int.to_string(state.total_hops))
  case state.completed_requests > 0 {
    True -> {
      let avg =
        int.to_float(state.total_hops) /. int.to_float(state.completed_requests)
      io.println("Average hops: " <> float.to_string(avg))
    }
    False -> io.println("No requests completed")
  }
  Nil
}

fn create_all_nodes(
  node_ids: List(Int),
) -> Result(
  #(
    List(process.Subject(NodeMessage)),
    Dict(NodeId, process.Subject(NodeMessage)),
  ),
  Nil,
) {
  let sorted_ids = sort_nodes(node_ids)
  let node_count = list.length(sorted_ids)
  let node_subjects =
    list.index_map(sorted_ids, fn(id, idx) {
      let next_idx = { idx + 1 } % node_count
      let successor = case list.drop(sorted_ids, next_idx) {
        [first, ..] -> first
        [] -> id
      }
      case start_node(id, successor, sorted_ids) {
        Ok(subject) -> Some(#(id, subject))
        Error(_) -> None
      }
    })
    |> list.filter_map(fn(opt) {
      case opt {
        Some(pair) -> Ok(pair)
        None -> Error(Nil)
      }
    })

  let registry = dict.from_list(node_subjects)
  let subjects = list.map(node_subjects, fn(pair) { pair.1 })

  // Configure nodes and set predecessors
  list.each(subjects, fn(subj) { process.send(subj, SetSelf(subj, registry)) })
  list.index_map(node_subjects, fn(pair, idx) {
    let #(_id, subject) = pair
    let prev_idx = case idx {
      0 -> node_count - 1
      _ -> idx - 1
    }
    case list.drop(node_subjects, prev_idx) {
      [#(pred_id, _), ..] -> process.send(subject, SetPredecessor(pred_id))
      [] -> Nil
    }
  })
  |> list.length()
  |> fn(_) { Nil }

  Ok(#(subjects, registry))
}

fn populate_initial_data(
  registry: Dict(NodeId, process.Subject(NodeMessage)),
  num_keys: Int,
) -> Nil {
  list.range(0, num_keys - 1)
  |> list.each(fn(i) {
    let key = mod_ring(i * 12_345)
    let value = "value_" <> int.to_string(i)
    let responsible = find_responsible_node(key, dict.keys(registry))
    case dict.get(registry, responsible) {
      Ok(node) -> process.send(node, Store(key, value))
      Error(_) -> Nil
    }
  })
}

pub fn run_simulation(num_nodes: Int, num_requests: Int) -> Nil {
  io.println("\n=== Chord P2P Simulation (Dynamic) ===")
  io.println("Nodes: " <> int.to_string(num_nodes))
  io.println("Requests per node: " <> int.to_string(num_requests))
  io.println(
    "Ring size (2^" <> int.to_string(m) <> "): " <> int.to_string(pow(2, m)),
  )
  io.println("")

  let node_ids = generate_node_ids(num_nodes)
  case create_all_nodes(node_ids) {
    Ok(#(nodes, registry)) -> {
      io.println("Created " <> int.to_string(list.length(nodes)) <> " nodes")
      io.println("Populating initial data...")
      populate_initial_data(registry, num_nodes * 2)
      // Allow stabilization and finger-table building to complete
      io.println("Stabilizing ring before starting requests...")
      process.sleep(15_000)

      // Coordinator
      let coord_subject = process.new_subject()
      let coord_state = init_coordinator(num_nodes, num_requests)

      // Start base traffic
      list.each(nodes, fn(subj) {
        process.send(subj, StartRequests(num_requests, coord_subject))
      })

      // --- Example: dynamically join a new node after 2 seconds (demo) ---
      // Comment out if you don't want demo join.
      // Demo join is disabled during the main measurement run.
      // If you want to demo joins later, set maybe_demo_join = True and
      // schedule JoinDemo BEFORE starting requests, then wait again.

      // Collect results
      let final_state = collect_results(coord_subject, coord_state)
      print_stats(final_state)

      io.println("\nShutting down nodes...")
      list.each(nodes, fn(s) { process.send(s, Shutdown) })
    }
    Error(_) -> io.println("Error creating nodes")
  }
  Nil
}

// We piggyback on existing actor by sending a Join with a brand new node.

// Patch: allow nodes to accept JoinDemo messages transparently by wrapping run
pub fn main() {
  case argv.load().arguments {
    [num_nodes_str, num_requests_str] -> {
      let num_nodes = int.parse(num_nodes_str) |> result.unwrap(10)
      let num_requests = int.parse(num_requests_str) |> result.unwrap(10)
      run_simulation(num_nodes, num_requests)
    }
    _ -> io.println("Usage: project3 numNodes numRequests")
  }
}
