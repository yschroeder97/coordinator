use crate::data_model::logical_source::{CreateLogicalSource, DropLogicalSource, LogicalSource};
use crate::data_model::schema::{DataType, Schema};
use crate::requests::RequestHeader;
use differential_dataflow::input::Input;
use differential_dataflow::operators::Join;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Node {
    host_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Link {
    src: String,
    dst: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Source {
    name: String,
    placement: String,
}

fn create_dataflow() {
    timely::execute_directly(move |worker| {
        let (mut node_handle, mut link_handle, mut source_handle) = worker.dataflow(|scope| {
            let (node_handle, nodes) = scope.new_collection::<Node, isize>();
            let (link_handle, links) = scope.new_collection::<Link, isize>();
            let (source_handle, sources) = scope.new_collection::<Source, isize>();
            
            let joined = sources.map(|source| (source.placement.clone(), source))
                .join(&nodes.map(|node| (node.host_name.clone(), node)))
                .inspect(|((_host, (source, node)), time, diff)| {
                    println!(
                        "At time {:?}: Source {} placed on node {} (diff: {})",
                        time, source.name, node.host_name, diff
                    );
                });

            (node_handle, link_handle, source_handle)
        });

        let source1 = Source {
            name: "source1".to_string(),
            placement: "node1".to_string(),
        };
        source_handle.advance_to(0);
        source_handle.insert(source1);

        let node1 = Node {
            host_name: "node1".to_string(),
        };
        let node2 = Node {
            host_name: "node2".to_string(),
        };
        node_handle.advance_to(1);
        node_handle.insert(node1);

        let link1 = Link {
            src: "node1".to_string(),
            dst: "node2".to_string(),
        };
        link_handle.advance_to(2);
        link_handle.insert(link1);
    });
}

#[cfg(test)]
mod differential_tests {
    use super::*;

    #[test]
    fn test_dataflow() {
        create_dataflow();
    }
}
