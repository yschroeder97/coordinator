use crate::worker::endpoint::HostAddr;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "network_link")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_host_addr: HostAddr,
    #[sea_orm(primary_key, auto_increment = false)]
    pub target_host_addr: HostAddr,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::Entity",
        from = "Column::SourceHostAddr",
        to = "super::Column::HostAddr",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    SourceWorker,
    #[sea_orm(
        belongs_to = "super::Entity",
        from = "Column::TargetHostAddr",
        to = "super::Column::HostAddr",
        on_update = "Cascade",
        on_delete = "Cascade"
    )]
    TargetWorker,
}

// Note: We intentionally don't implement Related<super::Entity> here because
// there are two foreign keys to Worker (source and target). Instead, use the
// Linked traits (WorkerToOutgoingNeighbors, WorkerToIncomingNeighbors) defined
// in the parent module for querying relationships.

impl ActiveModelBehavior for ActiveModel {}
