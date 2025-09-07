import gleam/erlang/atom

pub fn schedulers_online() -> Int {
  system_info(atom.create("schedulers_online"))
}

@external(erlang, "erlang", "system_info")
fn system_info(a: atom.Atom) -> Int



