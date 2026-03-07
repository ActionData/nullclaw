//! save_company — CRM tool to create or update company records with deduplication.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const resolve = @import("resolve.zig");

pub const SaveCompanyTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "save_company";
    pub const tool_description = "Create or update a company in the CRM. Searches for existing companies by name before creating. Returns disambiguation candidates if multiple matches found.";
    pub const tool_params =
        \\{"type":"object","properties":{"name":{"type":"string","description":"Company name"},"industry":{"type":"string","description":"Industry sector"},"size":{"type":"string","description":"Company size (e.g. startup, mid-market, enterprise)"},"website":{"type":"string","description":"Company website URL"},"notes":{"type":"string","description":"Free-text notes about this company"}},"required":["name"]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *SaveCompanyTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *SaveCompanyTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const db_inst = self.db orelse return root.ToolResult.fail("CRM database not configured");
        const db = db_inst.db orelse return root.ToolResult.fail("CRM database not open");

        const name = root.getString(args, "name") orelse return root.ToolResult.fail("Missing required parameter: name");
        if (name.len == 0) return root.ToolResult.fail("'name' must not be empty");

        const industry = root.getString(args, "industry");
        const size = root.getString(args, "size");
        const website = root.getString(args, "website");
        const notes = root.getString(args, "notes");

        // Resolve existing company by name using tiered matching
        var result = try resolve.resolveCompany(db_inst, allocator, name, null);
        defer resolve.freeResult(allocator, &result);

        switch (result) {
            .ambiguous => |candidates| {
                const msg = try resolve.formatCandidates(allocator, candidates, "company");
                defer allocator.free(msg);
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const w = buf.writer(allocator);
                try w.writeAll("{\"status\":\"disambiguation_needed\",\"message\":\"");
                try writeJsonEscaped(w, msg);
                try w.writeAll("\"}");
                return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
            },
            .resolved => |r| {
                return updateCompany(allocator, db, r.id, name, industry, size, website, notes);
            },
            .not_found => {
                return createCompany(allocator, db_inst, db, name, industry, size, website, notes);
            },
        }
    }

    fn updateCompany(allocator: std.mem.Allocator, db: *c.sqlite3, id: []const u8, name: []const u8, industry: ?[]const u8, size: ?[]const u8, website: ?[]const u8, notes: ?[]const u8) !root.ToolResult {
        const now = nowIso8601();

        const sql =
            \\UPDATE companies SET
            \\  name = ?1,
            \\  industry = COALESCE(?2, industry),
            \\  size = COALESCE(?3, size),
            \\  website = COALESCE(?4, website),
            \\  notes = COALESCE(?5, notes),
            \\  updated_at = ?6
            \\WHERE id = ?7;
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare update statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        bindOptionalText(stmt, 2, industry);
        bindOptionalText(stmt, 3, size);
        bindOptionalText(stmt, 4, website);
        bindOptionalText(stmt, 5, notes);
        _ = c.sqlite3_bind_text(stmt, 6, &now, now.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 7, id.ptr, @intCast(id.len), schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to update company");

        // Build updated fields list
        var fields_buf: std.ArrayList(u8) = .empty;
        defer fields_buf.deinit(allocator);
        const fw = fields_buf.writer(allocator);
        var first = true;
        if (industry != null) {
            try fw.writeAll("\"industry\"");
            first = false;
        }
        if (size != null) {
            if (!first) try fw.writeByte(',');
            try fw.writeAll("\"size\"");
            first = false;
        }
        if (website != null) {
            if (!first) try fw.writeByte(',');
            try fw.writeAll("\"website\"");
            first = false;
        }
        if (notes != null) {
            if (!first) try fw.writeByte(',');
            try fw.writeAll("\"notes\"");
        }

        return fetchAndReturnCompany(allocator, db, id, "updated", fields_buf.items);
    }

    fn createCompany(allocator: std.mem.Allocator, db_inst: *CrmDb, db: *c.sqlite3, name: []const u8, industry: ?[]const u8, size: ?[]const u8, website: ?[]const u8, notes: ?[]const u8) !root.ToolResult {
        const uuid = db_inst.generateUuid();
        const now = nowIso8601();

        const sql =
            \\INSERT INTO companies (id, name, industry, size, website, notes, created_at, updated_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare insert statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, &uuid, uuid.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        bindOptionalText(stmt, 3, industry);
        bindOptionalText(stmt, 4, size);
        bindOptionalText(stmt, 5, website);
        bindOptionalText(stmt, 6, notes);
        _ = c.sqlite3_bind_text(stmt, 7, &now, now.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 8, &now, now.len, schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to insert company");

        return fetchAndReturnCompany(allocator, db, &uuid, "created", null);
    }

    fn fetchAndReturnCompany(allocator: std.mem.Allocator, db: *c.sqlite3, id: []const u8, status: []const u8, fields_updated: ?[]const u8) !root.ToolResult {
        const sql = "SELECT id, name, industry, size, website, notes, created_at, updated_at FROM companies WHERE id = ?1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to fetch company");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return root.ToolResult.fail("Company not found after save");

        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.writeAll("{\"status\":\"");
        try w.writeAll(status);
        try w.writeAll("\",\"company\":{");
        try writeJsonField(w, "id", stmt, 0, true);
        try writeJsonField(w, "name", stmt, 1, false);
        try writeNullableField(w, "industry", stmt, 2);
        try writeNullableField(w, "size", stmt, 3);
        try writeNullableField(w, "website", stmt, 4);
        try writeNullableField(w, "notes", stmt, 5);
        try writeJsonField(w, "created_at", stmt, 6, false);
        try writeJsonField(w, "updated_at", stmt, 7, false);
        try w.writeByte('}');

        if (fields_updated) |fu| {
            if (fu.len > 0) {
                try w.writeAll(",\"fields_updated\":[");
                try w.writeAll(fu);
                try w.writeByte(']');
            }
        }

        try w.writeByte('}');
        return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
    }
};

// ── Shared helpers ──────────────────────────────────────────────────

pub fn nowIso8601() [20]u8 {
    const ts = std.time.timestamp();
    const epoch = std.time.epoch.EpochSeconds{ .secs = @intCast(ts) };
    const day = epoch.getDaySeconds();
    const yd = epoch.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    var buf: [20]u8 = undefined;
    _ = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        yd.year,
        md.month.numeric(),
        md.day_index + 1,
        day.getHoursIntoDay(),
        day.getMinutesIntoHour(),
        day.getSecondsIntoMinute(),
    }) catch unreachable;
    return buf;
}

pub fn bindOptionalText(stmt: ?*c.sqlite3_stmt, col: c_int, val: ?[]const u8) void {
    if (val) |v| {
        _ = c.sqlite3_bind_text(stmt, col, v.ptr, @intCast(v.len), schema.SQLITE_STATIC);
    } else {
        _ = c.sqlite3_bind_null(stmt, col);
    }
}

pub fn writeJsonField(w: anytype, field_name: []const u8, stmt: ?*c.sqlite3_stmt, col: c_int, first: bool) !void {
    if (!first) try w.writeByte(',');
    try w.writeByte('"');
    try w.writeAll(field_name);
    try w.writeAll("\":\"");
    const raw = c.sqlite3_column_text(stmt, col);
    if (raw != null) {
        try writeJsonEscapedC(w, raw);
    }
    try w.writeByte('"');
}

pub fn writeNullableField(w: anytype, field_name: []const u8, stmt: ?*c.sqlite3_stmt, col: c_int) !void {
    try w.writeAll(",\"");
    try w.writeAll(field_name);
    try w.writeAll("\":");
    const raw = c.sqlite3_column_text(stmt, col);
    if (raw != null) {
        try w.writeByte('"');
        try writeJsonEscapedC(w, raw);
        try w.writeByte('"');
    } else {
        try w.writeAll("null");
    }
}

pub fn writeJsonEscaped(w: anytype, s: []const u8) !void {
    for (s) |ch| {
        switch (ch) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            else => try w.writeByte(ch),
        }
    }
}

pub fn writeJsonEscapedC(w: anytype, raw: [*c]const u8) !void {
    if (raw == null) return;
    const s = std.mem.span(raw);
    try writeJsonEscaped(w, s);
}

pub fn writeNullableJsonC(w: anytype, raw: [*c]const u8) !void {
    if (raw == null) {
        try w.writeAll("null");
    } else {
        try w.writeByte('"');
        try writeJsonEscapedC(w, raw);
        try w.writeByte('"');
    }
}

// ── Tests ───────────────────────────────────────────────────────────

test "save_company tool name and description" {
    var t = SaveCompanyTool{};
    const tool = t.tool();
    try std.testing.expectEqualStrings("save_company", tool.name());
    try std.testing.expect(tool.description().len > 0);
}

test "save_company missing db" {
    var t = SaveCompanyTool{};
    const parsed = try root.parseTestArgs("{\"name\":\"Acme Corp\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
    try std.testing.expect(result.error_msg != null);
}

test "save_company missing name" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveCompanyTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"industry\":\"Tech\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "save_company create new" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveCompanyTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"Acme Corp\",\"industry\":\"Technology\",\"website\":\"https://acme.com\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Acme Corp") != null);
}

test "save_company duplicate detection" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    // Insert two companies with the same name
    try db.execSql("INSERT INTO companies (id, name, industry, created_at, updated_at) VALUES ('c1', 'Acme Corp', 'Tech', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");
    try db.execSql("INSERT INTO companies (id, name, industry, created_at, updated_at) VALUES ('c2', 'Acme Corp', 'Finance', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveCompanyTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"Acme Corp\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "disambiguation_needed") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Acme Corp") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Which one did you mean?") != null);
}

test "save_company update existing" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO companies (id, name, industry, created_at, updated_at) VALUES ('c1', 'Acme Corp', 'Tech', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveCompanyTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"Acme Corp\",\"website\":\"https://acme.com\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"updated\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "acme.com") != null);
}

test "save_company optional fields omitted" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveCompanyTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"MinimalCo\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "MinimalCo") != null);
}

test "nowIso8601 format" {
    const ts = nowIso8601();
    // Should be like "2026-03-07T14:30:00Z"
    try std.testing.expectEqual(@as(u8, '-'), ts[4]);
    try std.testing.expectEqual(@as(u8, '-'), ts[7]);
    try std.testing.expectEqual(@as(u8, 'T'), ts[10]);
    try std.testing.expectEqual(@as(u8, ':'), ts[13]);
    try std.testing.expectEqual(@as(u8, ':'), ts[16]);
    try std.testing.expectEqual(@as(u8, 'Z'), ts[19]);
}
