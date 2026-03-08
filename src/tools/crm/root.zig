//! CRM tools module — structured CRM data management for the Sales Agent.
//!
//! This module provides the CRM database schema and will export
//! tool implementations as they are added (e.g., crm-tools-core,
//! crm-tools-query).

pub const schema = @import("schema.zig");
pub const resolve = @import("resolve.zig");
pub const crm_memory = @import("crm_memory.zig");
pub const CrmDb = schema.CrmDb;

pub const save_contact = @import("save_contact.zig");
pub const save_company = @import("save_company.zig");
pub const save_deal = @import("save_deal.zig");
pub const log_activity = @import("log_activity.zig");

pub const SaveContactTool = save_contact.SaveContactTool;
pub const SaveCompanyTool = save_company.SaveCompanyTool;
pub const SaveDealTool = save_deal.SaveDealTool;
pub const LogActivityTool = log_activity.LogActivityTool;

pub const search_crm = @import("search_crm.zig");
pub const get_contact = @import("get_contact.zig");
pub const get_deal = @import("get_deal.zig");
pub const update_deal_stage = @import("update_deal_stage.zig");
pub const list_followups = @import("list_followups.zig");

pub const SearchCrmTool = search_crm.SearchCrmTool;
pub const GetContactTool = get_contact.GetContactTool;
pub const GetDealTool = get_deal.GetDealTool;
pub const UpdateDealStageTool = update_deal_stage.UpdateDealStageTool;
pub const ListFollowupsTool = list_followups.ListFollowupsTool;
