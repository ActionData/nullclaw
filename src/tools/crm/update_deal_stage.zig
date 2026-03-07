//! update_deal_stage tool — move a deal to a new pipeline stage with automatic history tracking.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;
const helpers = @import("save_company.zig");

pub const UpdateDealStageTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "update_deal_stage";
    pub const tool_description = "Update a deal's pipeline stage. Validates the stage, records the change in stage_history, and returns the updated deal summary.";
    pub const tool_params =
        \\{"type":"object","properties":{"deal":{"type":"string","description":"Deal title (resolved to deal_id)"},"deal_id":{"type":"string","description":"Deal UUID (takes precedence)"},"stage":{"type":"string","enum":["lead","qualification","proposal","negotiation","closed_won","closed_lost"],"description":"New pipeline stage"},"notes":{"type":"string","description":"Notes about this stage change"}},"required":["stage"]}
    ;

    const valid_stages = [_][]const u8{
        "lead",
        "qualification",
        "proposal",
        "negotiation",
        "closed_won",
        "closed_lost",
    };

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *UpdateDealStageTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *UpdateDealStageTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const deal_id = root.getString(args, "deal_id");
        const deal_title = root.getString(args, "deal");
        const new_stage = root.getString(args, "stage") orelse
            return root.ToolResult.fail("Missing required 'stage' parameter");
        const notes = root.getString(args, "notes") orelse "";

        if (deal_id == null and deal_title == null)
            return root.ToolResult.fail("At least one of 'deal' or 'deal_id' is required");

        // Validate stage
        var valid = false;
        for (valid_stages) |s| {
            if (std.mem.eql(u8, new_stage, s)) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            return root.ToolResult.fail("Invalid stage. Must be one of: lead, qualification, proposal, negotiation, closed_won, closed_lost");
        }

        const crm_db = self.db orelse
            return root.ToolResult.fail("CRM database not configured");

        const db = crm_db.db orelse
            return root.ToolResult.fail("CRM database not open");

        // Resolve deal
        var resolved_id: []const u8 = undefined;
        var resolved_title: []const u8 = undefined;
        var current_stage: []const u8 = undefined;
        var id_buf: [256]u8 = undefined;
        var title_buf: [256]u8 = undefined;
        var stage_buf: [64]u8 = undefined;

        if (deal_id) |did| {
            // Direct lookup
            const sql = "SELECT id, title, stage FROM deals WHERE id = ?1;";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare query");
            defer _ = c.sqlite3_finalize(stmt);

            _ = c.sqlite3_bind_text(stmt, 1, did.ptr, @intCast(did.len), SQLITE_STATIC);
            rc = c.sqlite3_step(stmt.?);
            if (rc != c.SQLITE_ROW) {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const bw = buf.writer(allocator);
                try bw.writeAll("{\"error\":\"Deal not found\",\"id\":\"");
                try helpers.writeJsonEscaped(bw, did);
                try bw.writeAll("\"}");
                return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
            }

            const id_text = columnText(stmt.?, 0);
            const title_text = columnText(stmt.?, 1);
            const stage_text = columnText(stmt.?, 2);
            @memcpy(id_buf[0..id_text.len], id_text);
            resolved_id = id_buf[0..id_text.len];
            @memcpy(title_buf[0..title_text.len], title_text);
            resolved_title = title_buf[0..title_text.len];
            @memcpy(stage_buf[0..stage_text.len], stage_text);
            current_stage = stage_buf[0..stage_text.len];
        } else {
            // Title-based lookup
            const sql = "SELECT id, title, stage FROM deals WHERE title LIKE ?1 COLLATE NOCASE LIMIT 1;";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare query");
            defer _ = c.sqlite3_finalize(stmt);

            const like_title = try std.fmt.allocPrint(allocator, "%{s}%", .{deal_title.?});
            defer allocator.free(like_title);
            _ = c.sqlite3_bind_text(stmt, 1, like_title.ptr, @intCast(like_title.len), SQLITE_STATIC);

            rc = c.sqlite3_step(stmt.?);
            if (rc != c.SQLITE_ROW) {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const bw = buf.writer(allocator);
                try bw.writeAll("{\"error\":\"No deal found matching '");
                try helpers.writeJsonEscaped(bw, deal_title.?);
                try bw.writeAll("'\"}");
                return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
            }

            const id_text = columnText(stmt.?, 0);
            const title_text = columnText(stmt.?, 1);
            const stage_text = columnText(stmt.?, 2);
            @memcpy(id_buf[0..id_text.len], id_text);
            resolved_id = id_buf[0..id_text.len];
            @memcpy(title_buf[0..title_text.len], title_text);
            resolved_title = title_buf[0..title_text.len];
            @memcpy(stage_buf[0..stage_text.len], stage_text);
            current_stage = stage_buf[0..stage_text.len];
        }

        // Check for no-op
        if (std.mem.eql(u8, current_stage, new_stage)) {
            var buf: std.ArrayList(u8) = .empty;
            errdefer buf.deinit(allocator);
            const w = buf.writer(allocator);
            try w.writeAll("{\"status\":\"no_change\",\"deal\":{\"id\":\"");
            try helpers.writeJsonEscaped(w, resolved_id);
            try w.writeAll("\",\"title\":\"");
            try helpers.writeJsonEscaped(w, resolved_title);
            try w.writeAll("\",\"stage\":\"");
            try helpers.writeJsonEscaped(w, current_stage);
            try w.writeAll("\"},\"message\":\"Deal is already in '");
            try helpers.writeJsonEscaped(w, new_stage);
            try w.writeAll("' stage\"}");
            return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
        }

        // Update deal stage and updated_at
        const now = helpers.nowIso8601();
        const update_sql = "UPDATE deals SET stage = ?1, updated_at = ?3 WHERE id = ?2;";
        var update_stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, update_sql, -1, &update_stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare update");
        defer _ = c.sqlite3_finalize(update_stmt);

        _ = c.sqlite3_bind_text(update_stmt, 1, new_stage.ptr, @intCast(new_stage.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(update_stmt, 2, resolved_id.ptr, @intCast(resolved_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(update_stmt, 3, &now, now.len, SQLITE_STATIC);

        rc = c.sqlite3_step(update_stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to update deal stage");

        // Insert stage history record
        const hist_id = crm_db.generateUuid();
        const hist_sql =
            \\INSERT INTO stage_history (id, deal_id, from_stage, to_stage, notes, changed_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        ;
        var hist_stmt: ?*c.sqlite3_stmt = null;
        rc = c.sqlite3_prepare_v2(db, hist_sql, -1, &hist_stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare history insert");
        defer _ = c.sqlite3_finalize(hist_stmt);

        _ = c.sqlite3_bind_text(hist_stmt, 1, &hist_id, 32, SQLITE_STATIC);
        _ = c.sqlite3_bind_text(hist_stmt, 2, resolved_id.ptr, @intCast(resolved_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(hist_stmt, 3, current_stage.ptr, @intCast(current_stage.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(hist_stmt, 4, new_stage.ptr, @intCast(new_stage.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(hist_stmt, 5, notes.ptr, @intCast(notes.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(hist_stmt, 6, &now, now.len, SQLITE_STATIC);

        rc = c.sqlite3_step(hist_stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to insert stage history");

        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        const w = buf.writer(allocator);
        try w.writeAll("{\"status\":\"updated\",\"deal\":{\"id\":\"");
        try helpers.writeJsonEscaped(w, resolved_id);
        try w.writeAll("\",\"title\":\"");
        try helpers.writeJsonEscaped(w, resolved_title);
        try w.writeAll("\",\"stage\":\"");
        try helpers.writeJsonEscaped(w, new_stage);
        try w.writeAll("\",\"previous_stage\":\"");
        try helpers.writeJsonEscaped(w, current_stage);
        try w.writeAll("\"},\"stage_history_entry\":{\"id\":\"");
        try w.writeAll(&hist_id);
        try w.writeAll("\",\"from_stage\":\"");
        try helpers.writeJsonEscaped(w, current_stage);
        try w.writeAll("\",\"to_stage\":\"");
        try helpers.writeJsonEscaped(w, new_stage);
        try w.writeAll("\",\"notes\":\"");
        try helpers.writeJsonEscaped(w, notes);
        try w.writeAll("\"}}");
        return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
    }

    fn columnText(stmt: *c.sqlite3_stmt, col: c_int) []const u8 {
        const ptr = c.sqlite3_column_text(stmt, col);
        if (ptr == null) return "";
        const len = c.sqlite3_column_bytes(stmt, col);
        if (len <= 0) return "";
        return ptr[0..@intCast(len)];
    }
};

// ── Tests ───────────────────────────────────────────────────────────

fn insertTestData(db: *c.sqlite3) !void {
    const inserts: [:0]const u8 =
        \\INSERT INTO companies (id, name, industry, size, created_at, updated_at)
        \\VALUES ('comp1', 'Northstar Technologies', 'SaaS', 'mid-market', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO contacts (id, name, company_id, role, created_at, updated_at)
        \\VALUES ('cont1', 'James Chen', 'comp1', 'VP Engineering', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO deals (id, title, company_id, contact_id, stage, value, currency, created_at, updated_at)
        \\VALUES ('deal1', 'Northstar Platform License', 'comp1', 'cont1', 'proposal', 45000.0, 'USD', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
    ;
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, inserts, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);
    if (rc != c.SQLITE_OK) return error.SqliteExecFailed;
}

test "update_deal_stage tool name" {
    var t = UpdateDealStageTool{};
    const tool_inst = t.tool();
    try std.testing.expectEqualStrings("update_deal_stage", tool_inst.name());
}

test "update_deal_stage missing stage" {
    var t = UpdateDealStageTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"deal_id\":\"deal1\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "update_deal_stage missing deal reference" {
    var t = UpdateDealStageTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"stage\":\"negotiation\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "update_deal_stage invalid stage" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = UpdateDealStageTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"deal_id\":\"deal1\",\"stage\":\"invalid_stage\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output.?, "Invalid stage") != null);
}

test "update_deal_stage success by id" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = UpdateDealStageTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"deal_id\":\"deal1\",\"stage\":\"negotiation\",\"notes\":\"SOW sent\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "updated") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "negotiation") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "proposal") != null);

    // Verify stage_history was created
    const check_sql = "SELECT count(*) FROM stage_history WHERE deal_id = 'deal1';";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, check_sql, -1, &stmt, null);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_OK), rc);
    defer _ = c.sqlite3_finalize(stmt);
    rc = c.sqlite3_step(stmt.?);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_ROW), rc);
    const count = c.sqlite3_column_int(stmt.?, 0);
    try std.testing.expectEqual(@as(c_int, 1), count);
}

test "update_deal_stage success by title" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = UpdateDealStageTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"deal\":\"Northstar Platform\",\"stage\":\"closed_won\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "closed_won") != null);
}

test "update_deal_stage no-op same stage" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = UpdateDealStageTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"deal_id\":\"deal1\",\"stage\":\"proposal\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "no_change") != null);
}

test "update_deal_stage deal not found" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = UpdateDealStageTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"deal_id\":\"nonexistent\",\"stage\":\"lead\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(!result.success);
}
