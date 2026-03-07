//! save_deal — CRM tool to create or update deal records with stage validation
//! and stage history tracking.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const helpers = @import("save_company.zig");

pub const SaveDealTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "save_deal";
    pub const tool_description = "Create or update a deal in the CRM pipeline. Validates stage against allowed values. Tracks stage changes in history. Resolves company and contact names to IDs.";
    pub const tool_params =
        \\{"type":"object","properties":{"title":{"type":"string","description":"Deal title"},"company":{"type":"string","description":"Company name (resolved to company_id)"},"company_id":{"type":"string","description":"Company UUID"},"contact":{"type":"string","description":"Contact name (resolved to contact_id)"},"contact_id":{"type":"string","description":"Contact UUID"},"stage":{"type":"string","enum":["lead","qualification","proposal","negotiation","closed_won","closed_lost"],"description":"Pipeline stage"},"value":{"type":"number","description":"Deal value"},"currency":{"type":"string","description":"Currency code (default: USD)"},"close_date":{"type":"string","description":"Expected close date (ISO 8601)"},"next_step":{"type":"string","description":"Next action item for this deal"},"notes":{"type":"string","description":"Free-text notes"}},"required":["title","stage"]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *SaveDealTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    const valid_stages = [_][]const u8{ "lead", "qualification", "proposal", "negotiation", "closed_won", "closed_lost" };

    fn isValidStage(stage: []const u8) bool {
        for (valid_stages) |vs| {
            if (std.mem.eql(u8, stage, vs)) return true;
        }
        return false;
    }

    pub fn execute(self: *SaveDealTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const db_inst = self.db orelse return root.ToolResult.fail("CRM database not configured");
        const db = db_inst.db orelse return root.ToolResult.fail("CRM database not open");

        const title = root.getString(args, "title") orelse return root.ToolResult.fail("Missing required parameter: title");
        if (title.len == 0) return root.ToolResult.fail("'title' must not be empty");

        const stage = root.getString(args, "stage") orelse return root.ToolResult.fail("Missing required parameter: stage");
        if (!isValidStage(stage)) return root.ToolResult.fail("Invalid stage. Must be one of: lead, qualification, proposal, negotiation, closed_won, closed_lost");

        // Resolve company
        var company_id: ?[]const u8 = root.getString(args, "company_id");
        const company_name = root.getString(args, "company");
        var company_id_owned = false;
        if (company_id == null and company_name != null) {
            company_id = try resolveByName(allocator, db, "companies", company_name.?);
            if (company_id != null) company_id_owned = true;
        }
        defer if (company_id_owned) if (company_id) |id| allocator.free(id);

        // Resolve contact
        var contact_id: ?[]const u8 = root.getString(args, "contact_id");
        const contact_name = root.getString(args, "contact");
        var contact_id_owned = false;
        if (contact_id == null and contact_name != null) {
            contact_id = try resolveByName(allocator, db, "contacts", contact_name.?);
            if (contact_id != null) contact_id_owned = true;
        }
        defer if (contact_id_owned) if (contact_id) |id| allocator.free(id);

        // Extract deal value (can be integer or float in JSON)
        const value: ?f64 = blk: {
            const val_json = root.getValue(args, "value") orelse break :blk null;
            break :blk switch (val_json) {
                .float => |f| f,
                .integer => |i| @as(f64, @floatFromInt(i)),
                else => null,
            };
        };

        const currency = root.getString(args, "currency");
        const close_date = root.getString(args, "close_date");
        const next_step = root.getString(args, "next_step");
        const notes = root.getString(args, "notes");

        // Search for existing deals by title (case-insensitive)
        const candidates = try searchByTitle(allocator, db, title);
        defer {
            for (candidates) |cand| freeDealCandidateFields(allocator, cand);
            allocator.free(candidates);
        }

        if (candidates.len > 1) {
            return buildDisambiguationResponse(allocator, title, candidates);
        }

        if (candidates.len == 1) {
            return updateDeal(allocator, db_inst, db, candidates[0], title, company_id, contact_id, stage, value, currency, close_date, next_step, notes);
        }

        return createDeal(allocator, db_inst, db, title, company_id, contact_id, stage, value, currency, close_date, next_step, notes);
    }

    fn resolveByName(allocator: std.mem.Allocator, db: *c.sqlite3, table: []const u8, name: []const u8) !?[]const u8 {
        // Build query — table name is a comptime-known string literal, safe to embed
        var sql_buf: [128]u8 = undefined;
        const sql_len = std.fmt.bufPrint(&sql_buf, "SELECT id FROM {s} WHERE name = ?1 COLLATE NOCASE LIMIT 1;", .{table}) catch return null;
        // Need null terminator
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
        return try allocator.dupe(u8, std.mem.span(raw));
    }

    fn resolveByTitle(allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) !?[]const u8 {
        const sql = "SELECT id FROM deals WHERE title = ?1 COLLATE NOCASE LIMIT 1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return null;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return null;

        const raw = c.sqlite3_column_text(stmt.?, 0);
        if (raw == null) return null;
        return try allocator.dupe(u8, std.mem.span(raw));
    }

    const DealCandidate = struct {
        id: []const u8,
        title: []const u8,
        stage: []const u8,
        company_id: []const u8,
    };

    fn freeDealCandidateFields(allocator: std.mem.Allocator, cand: DealCandidate) void {
        allocator.free(cand.id);
        if (cand.title.len > 0) allocator.free(cand.title);
        if (cand.stage.len > 0) allocator.free(cand.stage);
        if (cand.company_id.len > 0) allocator.free(cand.company_id);
    }

    fn searchByTitle(allocator: std.mem.Allocator, db: *c.sqlite3, title: []const u8) ![]DealCandidate {
        const sql = "SELECT id, title, stage, company_id FROM deals WHERE title = ?1 COLLATE NOCASE;";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, title.ptr, @intCast(title.len), schema.SQLITE_STATIC);

        var list: std.ArrayList(DealCandidate) = .empty;
        errdefer {
            for (list.items) |item| freeDealCandidateFields(allocator, item);
            list.deinit(allocator);
        }

        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            const id_raw = c.sqlite3_column_text(stmt.?, 0);
            if (id_raw == null) continue;
            const id_copy = try allocator.dupe(u8, std.mem.span(id_raw));
            errdefer allocator.free(id_copy);

            const title_raw = c.sqlite3_column_text(stmt.?, 1);
            const title_copy = if (title_raw != null) try allocator.dupe(u8, std.mem.span(title_raw)) else "";
            errdefer if (title_copy.len > 0) allocator.free(title_copy);

            const stage_raw = c.sqlite3_column_text(stmt.?, 2);
            const stage_copy = if (stage_raw != null) try allocator.dupe(u8, std.mem.span(stage_raw)) else "";
            errdefer if (stage_copy.len > 0) allocator.free(stage_copy);

            const cid_raw = c.sqlite3_column_text(stmt.?, 3);
            const cid_copy = if (cid_raw != null) try allocator.dupe(u8, std.mem.span(cid_raw)) else "";

            try list.append(allocator, .{
                .id = id_copy,
                .title = title_copy,
                .stage = stage_copy,
                .company_id = cid_copy,
            });
        }
        return list.toOwnedSlice(allocator);
    }

    fn buildDisambiguationResponse(allocator: std.mem.Allocator, title: []const u8, candidates: []const DealCandidate) !root.ToolResult {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.writeAll("{\"status\":\"disambiguation_needed\",\"message\":\"Multiple deals match '");
        try helpers.writeJsonEscaped(w, title);
        try w.writeAll("'. Please confirm which one, or indicate this is a new deal.\",\"candidates\":[");

        for (candidates, 0..) |cand, i| {
            if (i > 0) try w.writeByte(',');
            try w.writeAll("{\"id\":\"");
            try w.writeAll(cand.id);
            try w.writeAll("\",\"title\":");
            try helpers.writeNullableJsonSlice(w, cand.title);
            try w.writeAll(",\"stage\":");
            try helpers.writeNullableJsonSlice(w, cand.stage);
            try w.writeAll(",\"company_id\":");
            try helpers.writeNullableJsonSlice(w, cand.company_id);
            try w.writeByte('}');
        }
        try w.writeAll("]}");
        return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
    }

    fn updateDeal(allocator: std.mem.Allocator, db_inst: *CrmDb, db: *c.sqlite3, existing: DealCandidate, title: []const u8, company_id: ?[]const u8, contact_id: ?[]const u8, stage: []const u8, value: ?f64, currency: ?[]const u8, close_date: ?[]const u8, next_step: ?[]const u8, notes: ?[]const u8) !root.ToolResult {
        const now = helpers.nowIso8601();

        // Check if stage changed for history tracking
        const old_stage: ?[]const u8 = if (existing.stage.len > 0) existing.stage else null;
        const stage_changed = if (old_stage) |os| !std.mem.eql(u8, os, stage) else true;

        const sql =
            \\UPDATE deals SET
            \\  title = ?1,
            \\  company_id = COALESCE(?2, company_id),
            \\  contact_id = COALESCE(?3, contact_id),
            \\  stage = ?4,
            \\  value = COALESCE(?5, value),
            \\  currency = COALESCE(?6, currency),
            \\  close_date = COALESCE(?7, close_date),
            \\  next_step = COALESCE(?8, next_step),
            \\  notes = COALESCE(?9, notes),
            \\  updated_at = ?10
            \\WHERE id = ?11;
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare update statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, title.ptr, @intCast(title.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 2, company_id);
        helpers.bindOptionalText(stmt, 3, contact_id);
        _ = c.sqlite3_bind_text(stmt, 4, stage.ptr, @intCast(stage.len), schema.SQLITE_STATIC);
        if (value) |v| {
            _ = c.sqlite3_bind_double(stmt, 5, v);
        } else {
            _ = c.sqlite3_bind_null(stmt, 5);
        }
        helpers.bindOptionalText(stmt, 6, currency);
        helpers.bindOptionalText(stmt, 7, close_date);
        helpers.bindOptionalText(stmt, 8, next_step);
        helpers.bindOptionalText(stmt, 9, notes);
        _ = c.sqlite3_bind_text(stmt, 10, &now, now.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 11, existing.id.ptr, @intCast(existing.id.len), schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to update deal");

        // Insert stage history if stage changed
        if (stage_changed) {
            try insertStageHistory(db_inst, db, existing.id, old_stage, stage, &now);
        }

        return fetchAndReturnDeal(allocator, db, existing.id, "updated", stage_changed);
    }

    fn createDeal(allocator: std.mem.Allocator, db_inst: *CrmDb, db: *c.sqlite3, title: []const u8, company_id: ?[]const u8, contact_id: ?[]const u8, stage: []const u8, value: ?f64, currency: ?[]const u8, close_date: ?[]const u8, next_step: ?[]const u8, notes: ?[]const u8) !root.ToolResult {
        const uuid = db_inst.generateUuid();
        const now = helpers.nowIso8601();

        const sql =
            \\INSERT INTO deals (id, title, company_id, contact_id, stage, value, currency, close_date, next_step, notes, created_at, updated_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE(?7, 'USD'), ?8, ?9, ?10, ?11, ?12);
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare insert statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, &uuid, uuid.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, title.ptr, @intCast(title.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 3, company_id);
        helpers.bindOptionalText(stmt, 4, contact_id);
        _ = c.sqlite3_bind_text(stmt, 5, stage.ptr, @intCast(stage.len), schema.SQLITE_STATIC);
        if (value) |v| {
            _ = c.sqlite3_bind_double(stmt, 6, v);
        } else {
            _ = c.sqlite3_bind_null(stmt, 6);
        }
        helpers.bindOptionalText(stmt, 7, currency);
        helpers.bindOptionalText(stmt, 8, close_date);
        helpers.bindOptionalText(stmt, 9, next_step);
        helpers.bindOptionalText(stmt, 10, notes);
        _ = c.sqlite3_bind_text(stmt, 11, &now, now.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 12, &now, now.len, schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to insert deal");

        // Insert initial stage history (from_stage = null)
        try insertStageHistory(db_inst, db, &uuid, null, stage, &now);

        return fetchAndReturnDeal(allocator, db, &uuid, "created", true);
    }

    fn insertStageHistory(db_inst: *CrmDb, db: *c.sqlite3, deal_id: []const u8, from_stage: ?[]const u8, to_stage: []const u8, changed_at: []const u8) !void {
        const hist_uuid = db_inst.generateUuid();
        const sql =
            \\INSERT INTO stage_history (id, deal_id, from_stage, to_stage, changed_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5);
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, &hist_uuid, hist_uuid.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, deal_id.ptr, @intCast(deal_id.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 3, from_stage);
        _ = c.sqlite3_bind_text(stmt, 4, to_stage.ptr, @intCast(to_stage.len), schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 5, changed_at.ptr, @intCast(changed_at.len), schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return error.SqliteExecFailed;
    }

    fn fetchAndReturnDeal(allocator: std.mem.Allocator, db: *c.sqlite3, id: []const u8, status: []const u8, include_stage_history: bool) !root.ToolResult {
        const sql =
            \\SELECT d.id, d.title, d.company_id, co.name, d.contact_id, ct.name,
            \\       d.stage, d.value, d.currency, d.close_date, d.next_step, d.notes,
            \\       d.created_at, d.updated_at
            \\FROM deals d
            \\LEFT JOIN companies co ON d.company_id = co.id
            \\LEFT JOIN contacts ct ON d.contact_id = ct.id
            \\WHERE d.id = ?1;
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to fetch deal");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return root.ToolResult.fail("Deal not found after save");

        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.writeAll("{\"status\":\"");
        try w.writeAll(status);
        try w.writeAll("\",\"deal\":{");
        try helpers.writeJsonField(w, "id", stmt, 0, true);
        try helpers.writeJsonField(w, "title", stmt, 1, false);
        try helpers.writeNullableField(w, "company_id", stmt, 2);
        try helpers.writeNullableField(w, "company_name", stmt, 3);
        try helpers.writeNullableField(w, "contact_id", stmt, 4);
        try helpers.writeNullableField(w, "contact_name", stmt, 5);
        try helpers.writeJsonField(w, "stage", stmt, 6, false);

        // Value field (numeric)
        try w.writeAll(",\"value\":");
        if (c.sqlite3_column_type(stmt.?, 7) != c.SQLITE_NULL) {
            const val = c.sqlite3_column_double(stmt.?, 7);
            try std.fmt.format(w, "{d}", .{val});
        } else {
            try w.writeAll("null");
        }

        try helpers.writeNullableField(w, "currency", stmt, 8);
        try helpers.writeNullableField(w, "close_date", stmt, 9);
        try helpers.writeNullableField(w, "next_step", stmt, 10);
        try helpers.writeNullableField(w, "notes", stmt, 11);
        try helpers.writeJsonField(w, "created_at", stmt, 12, false);
        try helpers.writeJsonField(w, "updated_at", stmt, 13, false);
        try w.writeByte('}');

        // Include latest stage history entry
        if (include_stage_history) {
            try appendLatestStageHistory(w, db, id);
        }

        try w.writeByte('}');
        return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
    }

    fn appendLatestStageHistory(w: anytype, db: *c.sqlite3, deal_id: []const u8) !void {
        const sql = "SELECT from_stage, to_stage, changed_at FROM stage_history WHERE deal_id = ?1 ORDER BY changed_at DESC LIMIT 1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, deal_id.ptr, @intCast(deal_id.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return;

        try w.writeAll(",\"stage_history_entry\":{");
        try w.writeAll("\"from_stage\":");
        try helpers.writeNullableJsonC(w, c.sqlite3_column_text(stmt.?, 0));
        try w.writeAll(",\"to_stage\":");
        try helpers.writeNullableJsonC(w, c.sqlite3_column_text(stmt.?, 1));
        try w.writeAll(",\"changed_at\":");
        try helpers.writeNullableJsonC(w, c.sqlite3_column_text(stmt.?, 2));
        try w.writeByte('}');
    }
};

// ── Tests ───────────────────────────────────────────────────────────

test "save_deal tool name" {
    var t = SaveDealTool{};
    const tool = t.tool();
    try std.testing.expectEqualStrings("save_deal", tool.name());
}

test "save_deal missing title" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"stage\":\"lead\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "save_deal missing stage" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Big Deal\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "save_deal invalid stage" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Big Deal\",\"stage\":\"invalid_stage\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.error_msg.?, "Invalid stage") != null);
}

test "save_deal create new with stage history" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Platform License\",\"stage\":\"lead\",\"value\":50000,\"currency\":\"USD\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Platform License") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "stage_history_entry") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"from_stage\":null") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"to_stage\":\"lead\"") != null);

    // Verify stage_history row exists
    const check_sql = "SELECT count(*) FROM stage_history;";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, check_sql, -1, &stmt, null);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_OK), rc);
    defer _ = c.sqlite3_finalize(stmt);
    rc = c.sqlite3_step(stmt.?);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_ROW), rc);
    try std.testing.expectEqual(@as(c_int, 1), c.sqlite3_column_int(stmt.?, 0));
}

test "save_deal duplicate detection" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO deals (id, title, stage, created_at, updated_at) VALUES ('d1', 'Platform License', 'lead', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");
    try db.execSql("INSERT INTO deals (id, title, stage, created_at, updated_at) VALUES ('d2', 'Platform License', 'proposal', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Platform License\",\"stage\":\"lead\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "disambiguation_needed") != null);
}

test "save_deal update with stage change" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO deals (id, title, stage, value, created_at, updated_at) VALUES ('d1', 'Big Deal', 'lead', 10000, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Big Deal\",\"stage\":\"qualification\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"updated\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "stage_history_entry") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"to_stage\":\"qualification\"") != null);
}

test "save_deal optional fields omitted" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Minimal Deal\",\"stage\":\"lead\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
}

test "save_deal with company and contact resolution" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO companies (id, name, created_at, updated_at) VALUES ('co1', 'Northstar', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");
    try db.execSql("INSERT INTO contacts (id, name, created_at, updated_at) VALUES ('ct1', 'James Chen', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveDealTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"title\":\"Northstar License\",\"stage\":\"proposal\",\"company\":\"Northstar\",\"contact\":\"James Chen\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "co1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "ct1") != null);
}
