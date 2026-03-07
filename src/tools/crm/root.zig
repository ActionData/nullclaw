//! CRM tools module — structured CRM data management for the Sales Agent.
//!
//! This module provides the CRM database schema and will export
//! tool implementations as they are added (e.g., crm-tools-core,
//! crm-tools-query).

pub const schema = @import("schema.zig");
pub const resolve = @import("resolve.zig");
pub const CrmDb = schema.CrmDb;

pub const save_contact = @import("save_contact.zig");
pub const save_company = @import("save_company.zig");
pub const save_deal = @import("save_deal.zig");
pub const log_activity = @import("log_activity.zig");

pub const SaveContactTool = save_contact.SaveContactTool;
pub const SaveCompanyTool = save_company.SaveCompanyTool;
pub const SaveDealTool = save_deal.SaveDealTool;
pub const LogActivityTool = log_activity.LogActivityTool;
