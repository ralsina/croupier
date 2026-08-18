# Algorithm shamelessly stolen from https://stackoverflow.com/a/47234034
# Thanks Blckknght!

# Sort the vertices of `g` (an adjacency hash, vertex => the vertices it
# points at) starting from the "start" root, so every vertex comes after
# the vertices pointing at it.
#
# Neighbors are visited in sorted order, so the order among independent
# vertices is deterministic instead of following hash-table layout.
# `g[v]?` tolerates plain hashes without a default block (and doesn't
# mutate the graph by inserting missing keys on read).
def topological_sort(g)
  seen = Set(String).new
  stack = Array(String).new
  order = Array(String).new
  q = ["start"]
  while !q.empty?
    v = q.pop
    if !seen.includes?(v)
      seen << v
      (g[v]? || Set(String).new).to_a.sort.each do |w|
        q << w
      end
      while !stack.empty? && !(g[stack.last]? || Set(String).new).includes?(v)
        order << stack.pop
      end
      stack << v
    end
  end
  result = stack + order.reverse
  # The DFS visits every vertex reachable from "start". Anything never
  # seen (as a key or inside an adjacency list) is not reachable: that
  # is either an acyclic island (a wiring mistake like a missing
  # "start" edge) or an actual cycle. Report which, instead of calling
  # both a cycle.
  all_vertices = Set(String).new
  g.each do |vertex, neighbors|
    all_vertices << vertex
    all_vertices.concat neighbors
  end
  unvisited = all_vertices.reject { |vertex| seen.includes?(vertex) }
  unless unvisited.empty?
    if cyclic?(unvisited.to_a, g)
      raise "Cycle detected"
    end
    raise "Unreachable from start: #{unvisited.to_a.sort.join(", ")}"
  end
  result
end

# Whether the subgraph induced by `vertices` contains a cycle, via
# Kahn's algorithm: repeatedly peel off in-degree-zero vertices; any
# leftovers are on or behind a cycle.
private def cyclic?(vertices : Array(String), g)
  in_degree = vertices.to_h { |k| {k, 0} }
  vertices.each do |vertex|
    (g[vertex]? || Set(String).new).each do |neighbor|
      in_degree[neighbor] += 1 if in_degree.has_key?(neighbor)
    end
  end
  queue = in_degree.reject { |_, degree| degree > 0 }.keys
  peeled = 0
  while !queue.empty?
    vertex = queue.pop
    peeled += 1
    (g[vertex]? || Set(String).new).each do |neighbor|
      if in_degree.has_key?(neighbor) && (in_degree[neighbor] -= 1) == 0
        queue << neighbor
      end
    end
  end
  peeled != vertices.size
end
