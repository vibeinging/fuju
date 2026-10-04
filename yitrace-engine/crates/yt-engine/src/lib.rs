//! yt-engine —— 把各层串成一台引擎，并定义外部件的接口边界。
//!
//! 落地的设计：
//! - **单写者提交**：同一进程内所有改动 manifest 的提交都过 `WriteCoordinator` 锁；持久模式下再叠加
//!   data dir 内部的跨进程写锁。多个进程可以 `open_durable` 同一目录，但实际提交仍会串行。
//! - **段五态生命周期**（草案 1 §D1.2）：building → sealed → live → compacting → dead。
//! - **可替换的接口边界**：段存储、分词器、图向量索引都走 trait。默认实现是引擎内自研
//!   `ChineseTokenizer` + `DiskGraphIndex`；Vortex 和外部分词/图索引只作为可选适配层接入。
//! - **四源折叠读算子** `MergeOnReadExec` 的骨架：在固定快照上跨 memtable+段+deletion+upgrade
//!   归并，去重键 = 确定性 event_id。真实实现是 DataFusion 的 `ExecutionPlan`。
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use yt_core::chunk::{DeletionVec, UpgradeColChunk};
use yt_core::event::{EventIdentity, EventType};
use yt_core::fold::{fold_events, FoldInput, FoldedSpan, SpanFields};
use yt_core::ids::{SegmentId, WalLsn};
use yt_core::manifest::{Manifest, SegState, SegmentEntry};
use yt_manifest::{Current, Snapshot};
use yt_memtable::{MemRow, MemTable};
use yt_wal::{Wal, WalRecord};

mod wire;
pub use wire::parse_wire_batch;

mod otlp;
pub use otlp::parse_otlp_traces;

mod graph;
pub use graph::GraphAnnIndex;

mod bm25;
mod bm25_disk;
pub use bm25::{Bm25TextIndex, CjkBigramTokenizer, Tokenizer};

mod tokenizer_cn;
pub use tokenizer_cn::{ChineseTokenizer, Dict};

mod test_failpoints;

mod segstore;
pub use segstore::FileSegmentStore;

mod persist;
mod process_lock;
pub use process_lock::ProcessLockMetricsSnapshot;
mod vecstore;

mod gc_log;

pub mod olog;

mod event_order;
use event_order::SpanEventOrder;

mod filter_disk;
mod filter_external_sort;
mod filter_sidecar;
use filter_sidecar::FilterAttrsIndex;

mod metadata;
pub use metadata::{
    AnnotationStatus, AnnotationTarget, DatasetAssociation, DatasetAssociationFilter,
    NewDatasetAssociation, NewRetentionAuditRecord, NewRetentionPolicy, NewTraceAnnotation,
    RetentionAuditFilter, RetentionAuditRecord, RetentionPolicy, RetentionPolicyFilter,
    TraceAnnotation, TraceAnnotationFilter, UpdateTraceAnnotation,
};

mod metadata_index;
use metadata_index::MetadataIndex;

mod trace_rollup;
use trace_rollup::TraceAggregateRollupIndex;

mod vecindex_disk;
pub use vecindex_disk::{DiskGraphConfig, DiskGraphIndex, DiskGraphStore, DurableGraphIndex};

mod http;
pub use http::{EngineJsonApi, HttpIngestServer};

/// 编译期嵌入的控制台静态资源（build.rs 生成；console_dist/ 不存在则为空表）。
pub mod assets {
    include!(concat!(env!("OUT_DIR"), "/assets.rs"));
}

pub mod evalkit;

include!("engine/core_types.rs");
include!("engine/filter_types.rs");
include!("engine/public_views.rs");
include!("engine/eval_core.rs");
include!("engine/metadata_matchers.rs");
include!("engine/dataset_wire.rs");
include!("engine/coordinator_state.rs");
include!("engine/coordinator_builder.rs");

include!("engine/write_open.rs");
include!("engine/write_ingest_flush.rs");
include!("engine/write_read_query.rs");
include!("engine/write_sidecars.rs");
include!("engine/write_fold.rs");
include!("engine/write_console.rs");
include!("engine/write_eval_dataset.rs");
include!("engine/write_metadata.rs");
include!("engine/write_retention.rs");
include!("engine/write_graph_search.rs");
include!("engine/write_recover_commit.rs");

#[cfg(test)]
mod tests;
