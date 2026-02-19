use crate::worker::endpoint::HostAddr;
#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, Set};
use serde::{Deserialize, Serialize};
use strum::Display;

pub type SinkName = String;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub name: SinkName,
    pub host_addr: HostAddr,
    pub sink_type: SinkType,
    #[sea_orm(column_type = "Json")]
    pub config: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<crate::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug)]
pub struct CreateSink {
    pub name: SinkName,
    pub host_addr: HostAddr,
    pub sink_type: SinkType,
    pub config: serde_json::Value,
}

impl From<CreateSink> for ActiveModel {
    fn from(req: CreateSink) -> Self {
        Self {
            name: Set(req.name),
            host_addr: Set(req.host_addr),
            sink_type: Set(req.sink_type),
            config: Set(req.config),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetSink {
    pub by_name: Option<SinkName>,
    pub by_host_addr: Option<HostAddr>,
    pub by_type: Option<SinkType>,
}

impl GetSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn by_name(mut self, name: SinkName) -> Self {
        self.by_name = Some(name);
        self
    }

    pub fn by_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.by_host_addr = Some(host_addr);
        self
    }

    pub fn by_sink_type(mut self, sink_type: SinkType) -> Self {
        self.by_type = Some(sink_type);
        self
    }
}

impl crate::IntoCondition for GetSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.by_name.map(|v| Column::Name.eq(v)))
            .add_option(self.by_host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.by_type.map(|v| Column::SinkType.eq(v)))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DropSink {
    pub with_name: Option<SinkName>,
    pub with_host_addr: Option<HostAddr>,
    pub with_type: Option<SinkType>,
}

impl DropSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: SinkName) -> Self {
        self.with_name = Some(name);
        self
    }

    pub fn with_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.with_host_addr = Some(host_addr);
        self
    }

    pub fn with_sink_type(mut self, sink_type: SinkType) -> Self {
        self.with_type = Some(sink_type);
        self
    }
}

impl crate::IntoCondition for DropSink {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.with_name.map(|v| Column::Name.eq(v)))
            .add_option(self.with_host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.with_type.map(|v| Column::SinkType.eq(v)))
    }
}

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Display, Serialize, Deserialize, DeriveActiveEnum, EnumIter,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "sink_type",
    rename_all = "PascalCase"
)]
pub enum SinkType {
    File,
    Print,
}
