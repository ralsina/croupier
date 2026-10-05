# Iterative DFS topological sort, adapted from
# https://stackoverflow.com/a/47234034 (thanks, Blckknght).

module Croupier
  # Virtual root vertex of the task graph. The NUL byte keeps it from
  # colliding with any file path or task id.
  ROOT_VERTEX = "\0start"

  # Sort the vertices of `g` (an adjacency hash, vertex => the vertices it
  # points at) starting from the virtual root (ROOT_VERTEX), so every vertex
  # comes after the vertices pointing at it.
  #
  # Neighbors are visited in sorted order, so independent vertices
  # come out in a deterministic order. `g[v]?` works on hashes without
  # a default block and never inserts keys.
  #
  # Raises CycleError or UnreachableTaskError when some vertex is not
  # reachable from the root.
  def self.topological_sort(g)
    seen = Set(String).new
    stack = Array(String).new
    order = Array(String).new
    q = [ROOT_VERTEX]
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
    # Vertices the DFS never reached are either on a cycle or an
    # acyclic island with no edge from the root. Report which.
    all_vertices = Set(String).new
    g.each do |vertex, neighbors|
      all_vertices << vertex
      all_vertices.concat neighbors
    end
    unvisited = all_vertices.reject { |vertex| seen.includes?(vertex) }
    unless unvisited.empty?
      if cyclic?(unvisited.to_a, g)
        raise CycleError.new("Cycle detected in the task graph: #{unvisited.to_a.sort.join(", ")}")
      end
      raise UnreachableTaskError.new("Unreachable from root: #{unvisited.to_a.sort.join(", ")}")
    end
    result
  end

  # Whether the subgraph induced by `vertices` contains a cycle, via
  # Kahn's algorithm: repeatedly peel off in-degree-zero vertices; any
  # leftovers are on or behind a cycle.
  private def self.cyclic?(vertices : Array(String), g)
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
end

# Deprecated top-level alias, kept for compatibility.
@[Deprecated("Use `Croupier.topological_sort` instead")]
def topological_sort(g)
  Croupier.topological_sort(g)
end
