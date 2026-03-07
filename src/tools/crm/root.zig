//! CRM tools module — structured CRM data management for the Sales Agent.
//!
//! This module provides the CRM database schema and will export
//! tool implementations as they are added (e.g., crm-tools-core,
//! crm-tools-query).

pub const schema = @import("schema.zig");
pub const resolve = @import("resolve.zig");
pub const CrmDb = schema.CrmDb;
