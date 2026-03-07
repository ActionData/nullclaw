//! log_activity — CRM tool to log activities (meetings, calls, emails, notes)
//! with optional follow-up tracking.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const helpers = @import("save_company.zig");

pub const LogActivityTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "log_activity";
    pub const tool_description = "Log an activity (meeting, call, email, or note) in the CRM. Activities are append-only — each call creates a new record. Resolves contact, deal, and company names to IDs.";
    pub const tool_params =
        \\{"type":"object","properties":{"type":{"type":"string","enum":["meeting","call","email","note"],"description":"Activity type"},"contact":{"type":"string","description":"Contact name (resolved to contact_id)"},"contact_id":{"type":"string","description":"Contact UUID"},"deal":{"type":"string","description":"Deal title (resolved to deal_id)"},"deal_id":{"type":"string","description":"Deal UUID"},"company":{"type":"string","description":"Company name (resolved to company_id)"},"company_id":{"type":"string","description":"Company UUID"},"rep_id":{"type":"string","description":"Sales rep identifier"},"summary":{"type":"string","description":"Summary of the activity"},"date":{"type":"string","description":"Activity date (ISO 8601, defaults to now)"},"follow_up_date":{"type":"string","description":"Follow-up date (ISO 8601)"},"follow_up_note":{"type":"string","description":"Note about what the follow-up should cover"}},"required":["type","summary"]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *LogActivityTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    const valid_types = [_][]const u8{ "meeting", "call", "email", "note" };

    fn isValidType(activity_type: []const u8) bool {
        for (valid_types) |vt| {
            if (std.mem.eql(u8, activity_type, vt)) return true;
        }
        return false;
    }

    pub fn execute(self: *LogActivityTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const db_inst = self.db orelse return root.ToolResult.fail("CRM database not configured");
        const db = db_inst.db orelse return root.ToolResult.fail("CRM database not open");

        const activity_type = root.getString(args, "type") orelse return root.ToolResult.fail("Missing required parameter: type");
        if (!isValidType(activity_type)) return root.ToolResult.fail("Invalid type. Must be one of: meeting, call, email, note");

        const summary = root.getString(args, "summary") orelse return root.ToolResult.fail("Missing required parameter: summary");
        if (summary.len == 0) return root.ToolResult.fail("'summary' must not be empty");

        // Resolve contact
        var contact_id: ?[]const u8 = root.getString(args, "contact_id");
        const contact_name = root.getString(args, "contact");
        if (contact_id == null and contact_name != null) {
            contact_id = resolveByName(db, "contacts", contact_name.?);
        }

        // Resolve deal (by title)
        var deal_id: ?[]const u8 = root.getString(args, "deal_id");
        const deal_title = root.getString(args, "deal");
        if (deal_id == null and deal_title != null) {
            deal_id = resolveByTitle(db, deal_title.?);
        }

        // Resolve company
        var company_id: ?[]const u8 = root.getString(args, "company_id");
        const company_name = root.getString(args, "company");
        if (company_id == null and company_name != null) {
            company_id = resolveByName(db, "companies", company_name.?);
        }

        const rep_id = root.getString(args, "rep_id");

        // Default date to now if not provided
        const now = helpers.nowIso8601();
        const date = root.getString(args, "date") orelse &now;

        const follow_up_date = root.getString(args, "follow_up_date");
        const follow_up_note = root.getString(args, "follow_up_note");

        // Always create new (activities are append-only)
        const uuid = db_inst.generateUuid();

        const sql =
            \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, rep_id, summary, date, follow_up_date, follow_up_note, created_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11);
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare insert statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, &uuid, uuid.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, activity_type.ptr, @intCast(activity_type.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 3, contact_id);
        helpers.bindOptionalText(stmt, 4, deal_id);
        helpers.bindOptionalText(stmt, 5, company_id);
        helpers.bindOptionalText(stmt, 6, rep_id);
        _ = c.sqlite3_bind_text(stmt, 7, summary.ptr, @intCast(summary.len), schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 8, date.ptr, @intCast(date.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 9, follow_up_date);
        helpers.bindOptionalText(stmt, 10, follow_up_note);
        _ = c.sqlite3_bind_text(stmt, 11, &now, now.len, schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to insert activity");

        return fetchAndReturnActivity(allocator, db, &uuid);
    }

    fn resolveByName(db: *c.sqlite3, table: []const u8, name: []const u8) ?[]const u8 {
        var sql_buf: [128]u8 = undefined;
        const sql_len = std.fmt.bufPrint(&sql_buf, "SELECT id FROM {s} WHERE name = ?1 COLLATE NOCASE LIMIT 1;", .{table}) catch return null;
        var sql_z: [129]u8 = undefined;
        @memcpy(sql_z[0..sql_len.len], sql_len);
        sql_z[sql_len.len] = 0;

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, &sql_z, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return null;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return null;

        const raw = c.sqlite3_column_text(stmt.?, 0);
        if (raw == null) return null;
        return std.mem.span(raw);
    }

    fn resolveByTitle(db: *c.sqlite3, title: []const u8) ?[]const u8 {
        const sql = "SELECT id FROM deals WHERE title = ?1 COLLATE NOCASE LIMIT 1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return null;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, title.ptr, @intCast(title.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return null;

        const raw = c.sqlite3_column_text(stmt.?, 0);
        if (raw == null) return null;
        return std.mem.span(raw);
    }

    fn fetchAndReturnActivity(allocator: std.mem.Allocator, db: *c.sqlite3, id: []const u8) !root.ToolResult {
        const sql =
            \\SELECT a.id, a.type, a.contact_id, ct.name, a.deal_id, d.title,
            \\       a.company_id, co.name, a.rep_id, a.summary, a.date,
            \\       a.follow_up_date, a.follow_up_note, a.created_at
            \\FROM activities a
            \\LEFT JOIN contacts ct ON a.contact_id = ct.id
            \\LEFT JOIN deals d ON a.deal_id = d.id
            \\LEFT JOIN companies co ON a.company_id = co.id
            \\WHERE a.id = ?1;
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to fetch activity");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return root.ToolResult.fail("Activity not found after save");

        var buf = std.ArrayList(u8).init(allocator);
        errdefer buf.deinit();
        const w = buf.writer();

        try w.writeAll("{\"status\":\"created\",\"activity\":{");
        try helpers.writeJsonField(w, "id", stmt, 0, true);
        try helpers.writeJsonField(w, "type", stmt, 1, false);
        try helpers.writeNullableField(w, "contact_id", stmt, 2);
        try helpers.writeNullableField(w, "contact_name", stmt, 3);
        try helpers.writeNullableField(w, "deal_id", stmt, 4);
        try helpers.writeNullableField(w, "deal_title", stmt, 5);
        try helpers.writeNullableField(w, "company_id", stmt, 6);
        try helpers.writeNullableField(w, "company_name", stmt, 7);
        try helpers.writeNullableField(w, "rep_id", stmt, 8);
        try helpers.writeJsonField(w, "summary", stmt, 9, false);
        try helpers.writeJsonField(w, "date", stmt, 10, false);
        try helpers.writeNullableField(w, "follow_up_date", stmt, 11);
        try helpers.writeNullableField(w, "follow_up_note", stmt, 12);
        try helpers.writeJsonField(w, "created_at", stmt, 13, false);
        try w.writeAll("}}");

        return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice() };
    }
};

// ── Tests ───────────────────────────────────────────────────────────

test "log_activity tool name" {
    var t = LogActivityTool{};
    const tool = t.tool();
    try std.testing.expectEqualStrings("log_activity", tool.name());
}

test "log_activity missing type" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"summary\":\"Had a meeting\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "log_activity missing summary" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"meeting\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "log_activity invalid type" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"invalid\",\"summary\":\"Test\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "Invalid type") != null);
}

test "log_activity create new" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"meeting\",\"summary\":\"Discussed implementation timeline\",\"rep_id\":\"U02ABC123\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Discussed implementation timeline") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "U02ABC123") != null);
}

test "log_activity defaults date to now" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"call\",\"summary\":\"Quick check-in\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // Date should be set (contains T and Z from ISO8601)
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"date\":\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "T") != null);
}

test "log_activity with explicit date" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"email\",\"summary\":\"Sent proposal\",\"date\":\"2026-03-01T10:00:00Z\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "2026-03-01T10:00:00Z") != null);
}

test "log_activity with follow-up" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"meeting\",\"summary\":\"Kickoff meeting\",\"follow_up_date\":\"2026-03-14\",\"follow_up_note\":\"Send SOW draft\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "2026-03-14") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Send SOW draft") != null);
}

test "log_activity with entity resolution" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO companies (id, name, created_at, updated_at) VALUES ('co1', 'Northstar', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");
    try db.execSql("INSERT INTO contacts (id, name, created_at, updated_at) VALUES ('ct1', 'James Chen', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");
    try db.execSql("INSERT INTO deals (id, title, stage, created_at, updated_at) VALUES ('d1', 'Big Deal', 'lead', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"note\",\"summary\":\"Progress update\",\"contact\":\"James Chen\",\"deal\":\"Big Deal\",\"company\":\"Northstar\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "co1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "ct1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "d1") != null);
}

test "log_activity optional fields omitted" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = LogActivityTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"type\":\"note\",\"summary\":\"Just a note\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
}
