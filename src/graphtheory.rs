use anyhow::{anyhow, Result};
use petgraph::{
    algo::toposort,
    graph::{DiGraph, NodeIndex},
};

use std::collections::HashSet;

/// Returns all nodes at a fixed `distance` from `source` in `G`.
/// https://github.com/networkx/networkx/blob/cabf22e98d06d6c34ff88515f339b515695a7455/networkx/algorithms/traversal/breadth_first_search.py#L372
pub fn descendants_at_distance<N, V>(
    graph: &DiGraph<N, V>,
    source: NodeIndex,
    distance: i32,
) -> HashSet<NodeIndex> {
    let mut current_distance = 0;
    let mut queue = HashSet::new();
    let mut visited = HashSet::new();

    queue.insert(source);
    visited.insert(source);

    // this is basically BFS, except that the queue only stores the nodes at
    // current_distance from source at each iteration
    while !queue.is_empty() {
        if current_distance == distance {
            return queue;
        }

        current_distance += 1;
        let mut next_vertices = HashSet::new();
        for vertex in queue {
            for child in graph.neighbors(vertex) {
                if !visited.contains(&child) {
                    visited.insert(child);
                    next_vertices.insert(child);
                }
            }
        }

        queue = next_vertices;
    }

    HashSet::new()
}

/// Returns the transitive closure of a directed acyclic graph.
/// This function fails if the graph has a cycle.
/// The transitive closure of G = (V,E) is a graph G+ = (V,E+) such that
/// for all v, w in V there is an edge (v, w) in E+ if and only if there
/// is a non-null path from v to w in G.
/// https://github.com/networkx/networkx/blob/cabf22e98d06d6c34ff88515f339b515695a7455/networkx/algorithms/dag.py#L581
pub fn transitive_closure_dag<N: Clone + Default, V>(
    graph: &DiGraph<N, V>,
) -> Result<DiGraph<N, ()>> {
    let mut tc = graph.map(|_, w| w.clone(), |_, _| ());
    let toposort =
        toposort(&graph, None).map_err(|e| anyhow!("Graph contains a cycle: {:?}", e))?;

    for &v in toposort.iter().rev() {
        tc.extend_with_edges(descendants_at_distance(&tc, v, 2).iter().map(|&u| (v, u)));
    }

    Ok(tc)
}

#[cfg(test)]
mod test {
    use crate::graphtheory::transitive_closure_dag;
    use petgraph::graph::DiGraph;

    fn sorted_tc_dag_edges<N: Copy + Default, E>(g: DiGraph<N, E>) -> Vec<(usize, usize)> {
        let mut tc = transitive_closure_dag(&g)
            .unwrap()
            .raw_edges()
            .iter()
            .map(|e| (e.source().index(), e.target().index()))
            .collect::<Vec<(usize, usize)>>();
        tc.sort();
        tc
    }

    #[test]
    fn test_transitive_closure_dag_1() {
        let mut g: DiGraph<i32, ()> = DiGraph::new();
        g.extend_with_edges(&[(0, 1), (1, 2), (2, 3)]);
        let tc = sorted_tc_dag_edges(g);
        assert_eq!(tc, vec![(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
    }

    #[test]
    fn test_transitive_closure_dag_2() {
        let mut g: DiGraph<i32, ()> = DiGraph::new();
        g.extend_with_edges(&[(0, 1), (1, 2), (1, 3)]);
        let tc = sorted_tc_dag_edges(g);
        assert_eq!(tc, vec![(0, 1), (0, 2), (0, 3), (1, 2), (1, 3)]);
    }
}
