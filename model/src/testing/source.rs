use crate::source::logical_source::CreateLogicalSource;
use crate::source::physical_source::CreatePhysicalSource;
use crate::source::physical_source::SourceType;
use crate::worker::CreateWorker;
use crate::worker::arb_create_worker;
use proptest::prelude::*;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct PhysicalSourceWithRefs {
    pub logical: CreateLogicalSource,
    pub worker: CreateWorker,
    pub physical: CreatePhysicalSource,
}

prop_compose! {
    pub fn arb_physical_with_refs()(
        logical in any::<CreateLogicalSource>(),
        worker in arb_create_worker(),
        source_type in any::<SourceType>(),
    ) -> PhysicalSourceWithRefs {
        let physical = CreatePhysicalSource {
            logical_source: logical.name.clone(),
            host_addr: worker.host_addr.clone(),
            source_type,
            source_config: json!({}),
            parser_config: json!({}),
        };
        PhysicalSourceWithRefs { logical, worker, physical }
    }
}
