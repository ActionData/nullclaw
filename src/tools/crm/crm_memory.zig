//! crm_memory — shared helpers for writing CRM data summaries to nullclaw's memory store.
//!
//! After each CRM write operation, a formatted text summary is pushed to long-term
//! memory for RAG retrieval. Memory entries use `.core` category (stable facts) and
//! `null` session_id (globally accessible across sessions).

const std = @import("std");
const mem_root = @import("../../memory/root.zig");
pub const Memory = mem_root.Memory;
pub const MemoryRuntime = mem_root.MemoryRuntime;
pub const MemoryCategory = mem_root.MemoryCategory;

/// Store a CRM memory entry and trigger vector sync (best-effort).
/// Failures are silently caught — the CRM SQLite write is authoritative.
pub fn storeCrmMemory(
    allocator: std.mem.Allocator,
    memory: Memory,
    mem_rt: ?*MemoryRuntime,
    key: []const u8,
    content: []const u8,
) void {
    memory.store(key, content, .core, null) catch {};
    if (mem_rt) |rt| {
        rt.syncVectorAfterStore(allocator, key, content);
    }
}

/// Format: [CONTACT] {name}, {role} at {company}. {notes}
pub fn formatContact(
    allocator: std.mem.Allocator,
    name: []const u8,
    role: ?[]const u8,
    company_name: ?[]const u8,
    notes: ?[]const u8,
) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("[CONTACT] ");
    try w.writeAll(name);

    if (role) |r| if (r.len > 0) {
        try w.writeAll(", ");
        try w.writeAll(r);
    };

    if (company_name) |cn| if (cn.len > 0) {
        try w.writeAll(" at ");
        try w.writeAll(cn);
    };

    try w.writeByte('.');

    if (notes) |n| if (n.len > 0) {
        try w.writeByte(' ');
        try w.writeAll(n);
    };

    return buf.toOwnedSlice(allocator);
}

/// Format: [COMPANY] {name}. Industry: {industry}. Size: {size}. {notes}
pub fn formatCompany(
    allocator: std.mem.Allocator,
    name: []const u8,
    industry: ?[]const u8,
    size: ?[]const u8,
    notes: ?[]const u8,
) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("[COMPANY] ");
    try w.writeAll(name);
    try w.writeByte('.');

    if (industry) |ind| if (ind.len > 0) {
        try w.writeAll(" Industry: ");
        try w.writeAll(ind);
        try w.writeByte('.');
    };

    if (size) |s| if (s.len > 0) {
        try w.writeAll(" Size: ");
        try w.writeAll(s);
        try w.writeByte('.');
    };

    if (notes) |n| if (n.len > 0) {
        try w.writeByte(' ');
        try w.writeAll(n);
    };

    return buf.toOwnedSlice(allocator);
}

/// Format: [DEAL] {title} with {company}. Stage: {stage}. Value: {value} {currency}. {notes}
pub fn formatDeal(
    allocator: std.mem.Allocator,
    title: []const u8,
    company_name: ?[]const u8,
    stage: []const u8,
    value: ?f64,
    currency: ?[]const u8,
    notes: ?[]const u8,
) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("[DEAL] ");
    try w.writeAll(title);

    if (company_name) |cn| if (cn.len > 0) {
        try w.writeAll(" with ");
        try w.writeAll(cn);
    };

    try w.writeAll(". Stage: ");
    try w.writeAll(stage);
    try w.writeByte('.');

    if (value) |v| {
        try w.writeAll(" Value: ");
        try std.fmt.format(w, "{d}", .{v});
        try w.writeByte(' ');
        try w.writeAll(currency orelse "USD");
        try w.writeByte('.');
    }

    if (notes) |n| if (n.len > 0) {
        try w.writeByte(' ');
        try w.writeAll(n);
    };

    return buf.toOwnedSlice(allocator);
}

/// Format: [{TYPE}] {date} with {contact} at {company}: {summary}
pub fn formatActivity(
    allocator: std.mem.Allocator,
    activity_type: []const u8,
    date: []const u8,
    contact_name: ?[]const u8,
    company_name: ?[]const u8,
    summary: []const u8,
) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeByte('[');
    // Uppercase the activity type
    for (activity_type) |ch| {
        try w.writeByte(std.ascii.toUpper(ch));
    }
    try w.writeAll("] ");
    try w.writeAll(date);

    if (contact_name) |cn| if (cn.len > 0) {
        try w.writeAll(" with ");
        try w.writeAll(cn);
    };

    if (company_name) |cn| if (cn.len > 0) {
        try w.writeAll(" at ");
        try w.writeAll(cn);
    };

    try w.writeAll(": ");
    try w.writeAll(summary);

    return buf.toOwnedSlice(allocator);
}

/// Format: [FOLLOW-UP] Follow-up for {contact}/{company} on {date}: {note}
pub fn formatFollowUp(
    allocator: std.mem.Allocator,
    contact_name: ?[]const u8,
    company_name: ?[]const u8,
    follow_up_date: []const u8,
    follow_up_note: ?[]const u8,
) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("[FOLLOW-UP] Follow-up for ");

    if (contact_name) |cn| if (cn.len > 0) {
        try w.writeAll(cn);
    };

    if (company_name) |cn| if (cn.len > 0) {
        if (contact_name != null and contact_name.?.len > 0) {
            try w.writeByte('/');
        }
        try w.writeAll(cn);
    };

    try w.writeAll(" on ");
    try w.writeAll(follow_up_date);
    try w.writeAll(": ");

    if (follow_up_note) |note| if (note.len > 0) {
        try w.writeAll(note);
    };

    return buf.toOwnedSlice(allocator);
}

/// Format: [STAGE] {title} moved from {from_stage} to {to_stage}. {notes}
pub fn formatStageChange(
    allocator: std.mem.Allocator,
    title: []const u8,
    from_stage: []const u8,
    to_stage: []const u8,
    notes: []const u8,
) ![]const u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.writeAll("[STAGE] ");
    try w.writeAll(title);
    try w.writeAll(" moved from ");
    try w.writeAll(from_stage);
    try w.writeAll(" to ");
    try w.writeAll(to_stage);
    try w.writeByte('.');

    if (notes.len > 0) {
        try w.writeByte(' ');
        try w.writeAll(notes);
    }

    return buf.toOwnedSlice(allocator);
}

// ── Tests ───────────────────────────────────────────────────────────

test "formatContact full" {
    const result = try formatContact(std.testing.allocator, "James Chen", "VP Engineering", "Northstar", "Key decision maker");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[CONTACT] James Chen, VP Engineering at Northstar. Key decision maker", result);
}

test "formatContact minimal" {
    const result = try formatContact(std.testing.allocator, "Jane Doe", null, null, null);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[CONTACT] Jane Doe.", result);
}

test "formatCompany full" {
    const result = try formatCompany(std.testing.allocator, "Acme Corp", "Technology", "enterprise", "Fortune 500");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[COMPANY] Acme Corp. Industry: Technology. Size: enterprise. Fortune 500", result);
}

test "formatCompany minimal" {
    const result = try formatCompany(std.testing.allocator, "MinimalCo", null, null, null);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[COMPANY] MinimalCo.", result);
}

test "formatDeal full" {
    const result = try formatDeal(std.testing.allocator, "Platform License", "Northstar", "proposal", 50000.0, "USD", "High priority");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[DEAL] Platform License with Northstar. Stage: proposal. Value: 50000 USD. High priority", result);
}

test "formatDeal minimal" {
    const result = try formatDeal(std.testing.allocator, "Simple Deal", null, "lead", null, null, null);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[DEAL] Simple Deal. Stage: lead.", result);
}

test "formatActivity meeting" {
    const result = try formatActivity(std.testing.allocator, "meeting", "2026-03-07T10:00:00Z", "James Chen", "Northstar", "Discussed timeline");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[MEETING] 2026-03-07T10:00:00Z with James Chen at Northstar: Discussed timeline", result);
}

test "formatFollowUp" {
    const result = try formatFollowUp(std.testing.allocator, "James Chen", "Northstar", "2026-03-14", "Send SOW draft");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[FOLLOW-UP] Follow-up for James Chen/Northstar on 2026-03-14: Send SOW draft", result);
}

test "formatStageChange" {
    const result = try formatStageChange(std.testing.allocator, "Platform License", "proposal", "negotiation", "SOW sent");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[STAGE] Platform License moved from proposal to negotiation. SOW sent", result);
}

test "formatStageChange no notes" {
    const result = try formatStageChange(std.testing.allocator, "Big Deal", "lead", "qualification", "");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("[STAGE] Big Deal moved from lead to qualification.", result);
}
