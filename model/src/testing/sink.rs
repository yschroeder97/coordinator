use crate::sink::{CreateSink, SinkType};
use crate::source::schema::Schema;
use crate::worker::CreateWorker;
use crate::worker::arb_create_worker;
use proptest::prelude::*;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct SinkWithRefs {
    pub worker: CreateWorker,
    pub sink: CreateSink,
}

prop_compose! {
    pub fn arb_sink_with_refs()(
        name in proptest::string::string_regex("[a-z][a-z0-9_]{2,29}").unwrap(),
        worker in arb_create_worker(),
        sink_type in any::<SinkType>(),
        schema in any::<Schema>(),
    ) -> SinkWithRefs {
        let sink = CreateSink {
            name,
            host_addr: worker.host_addr.clone(),
            sink_type,
            schema,
            config: json!({}),
        };
        SinkWithRefs { worker, sink }
    }
}
