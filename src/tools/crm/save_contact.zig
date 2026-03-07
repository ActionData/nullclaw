//! save_contact — CRM tool to create or update contact records with deduplication.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const helpers = @import("save_company.zig");

pub const SaveContactTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "save_contact";
    pub const tool_description = "Create or update a contact in the CRM. Searches for existing contacts by name before creating. Returns disambiguation candidates if multiple matches found.";
    pub const tool_params =
        \\{"type":"object","properties":{"name":{"type":"string","description":"Contact's full name"},"company":{"type":"string","description":"Company name (resolved to company_id)"},"company_id":{"type":"string","description":"Company UUID (takes precedence over name)"},"role":{"type":"string","description":"Contact's role or title"},"email":{"type":"string","description":"Email address"},"phone":{"type":"string","description":"Phone number"},"notes":{"type":"string","description":"Free-text notes about this contact"}},"required":["name"]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *SaveContactTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *SaveContactTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const db_inst = self.db orelse return root.ToolResult.fail("CRM database not configured");
        const db = db_inst.db orelse return root.ToolResult.fail("CRM database not open");

        const name = root.getString(args, "name") orelse return root.ToolResult.fail("Missing required parameter: name");
        if (name.len == 0) return root.ToolResult.fail("'name' must not be empty");

        // Resolve company: company_id takes precedence, else search by name
        var resolved_company_id: ?[]const u8 = root.getString(args, "company_id");
        const company_name = root.getString(args, "company");

        if (resolved_company_id == null and company_name != null) {
            resolved_company_id = try resolveCompanyByName(allocator, db, company_name.?);
        }

        const role = root.getString(args, "role");
        const email = root.getString(args, "email");
        const phone = root.getString(args, "phone");
        const notes = root.getString(args, "notes");

        // Search for existing contacts by name (case-insensitive)
        const candidates = try searchByName(allocator, db, name);
        defer {
            for (candidates) |cand| allocator.free(cand.id);
            allocator.free(candidates);
        }

        if (candidates.len > 1) {
            return buildDisambiguationResponse(allocator, name, candidates);
        }

        if (candidates.len == 1) {
            return updateContact(allocator, db, candidates[0].id, name, resolved_company_id, role, email, phone, notes);
        }

        return createContact(allocator, db_inst, db, name, resolved_company_id, role, email, phone, notes);
    }

    fn resolveCompanyByName(allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) !?[]const u8 {
        _ = allocator;
        const sql = "SELECT id FROM companies WHERE name = ?1 COLLATE NOCASE LIMIT 1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return null;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return null;

        const raw = c.sqlite3_column_text(stmt.?, 0);
        if (raw == null) return null;
        return std.mem.span(raw);
    }

    const Candidate = struct {
        id: []const u8,
        name: [*c]const u8,
        company_id: [*c]const u8,
        role: [*c]const u8,
    };

    fn searchByName(allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) ![]Candidate {
        const sql = "SELECT id, name, company_id, role FROM contacts WHERE name = ?1 COLLATE NOCASE;";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);

        var list: std.ArrayList(Candidate) = .empty;
        errdefer {
            for (list.items) |item| allocator.free(item.id);
            list.deinit(allocator);
        }

        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            const id_raw = c.sqlite3_column_text(stmt.?, 0);
            if (id_raw == null) continue;
            const id_copy = try allocator.dupe(u8, std.mem.span(id_raw));

            try list.append(allocator, .{
                .id = id_copy,
                .name = c.sqlite3_column_text(stmt.?, 1),
                .company_id = c.sqlite3_column_text(stmt.?, 2),
                .role = c.sqlite3_column_text(stmt.?, 3),
            });
        }
        return list.toOwnedSlice(allocator);
    }

    fn buildDisambiguationResponse(allocator: std.mem.Allocator, name: []const u8, candidates: []const Candidate) !root.ToolResult {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.writeAll("{\"status\":\"disambiguation_needed\",\"message\":\"Multiple contacts match '");
        try helpers.writeJsonEscaped(w, name);
        try w.writeAll("'. Please confirm which one, or indicate this is a new contact.\",\"candidates\":[");

        for (candidates, 0..) |cand, i| {
            if (i > 0) try w.writeByte(',');
            try w.writeAll("{\"id\":\"");
            try w.writeAll(cand.id);
            try w.writeAll("\",\"name\":");
            try helpers.writeNullableJsonC(w, cand.name);
            try w.writeAll(",\"company_id\":");
            try helpers.writeNullableJsonC(w, cand.company_id);
            try w.writeAll(",\"role\":");
            try helpers.writeNullableJsonC(w, cand.role);
            try w.writeByte('}');
        }
        try w.writeAll("]}");
        return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
    }

    fn updateContact(allocator: std.mem.Allocator, db: *c.sqlite3, id: []const u8, name: []const u8, company_id: ?[]const u8, role: ?[]const u8, email: ?[]const u8, phone: ?[]const u8, notes: ?[]const u8) !root.ToolResult {
        const now = helpers.nowIso8601();

        const sql =
            \\UPDATE contacts SET
            \\  name = ?1,
            \\  company_id = COALESCE(?2, company_id),
            \\  role = COALESCE(?3, role),
            \\  email = COALESCE(?4, email),
            \\  phone = COALESCE(?5, phone),
            \\  notes = COALESCE(?6, notes),
            \\  updated_at = ?7
            \\WHERE id = ?8;
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare update statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 2, company_id);
        helpers.bindOptionalText(stmt, 3, role);
        helpers.bindOptionalText(stmt, 4, email);
        helpers.bindOptionalText(stmt, 5, phone);
        helpers.bindOptionalText(stmt, 6, notes);
        _ = c.sqlite3_bind_text(stmt, 7, &now, now.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 8, id.ptr, @intCast(id.len), schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to update contact");

        // Build fields_updated list
        var fields_buf: std.ArrayList(u8) = .empty;
        defer fields_buf.deinit(allocator);
        const fw = fields_buf.writer(allocator);
        var first = true;
        const field_names = [_]struct { name: []const u8, val: ?[]const u8 }{
            .{ .name = "company_id", .val = company_id },
            .{ .name = "role", .val = role },
            .{ .name = "email", .val = email },
            .{ .name = "phone", .val = phone },
            .{ .name = "notes", .val = notes },
        };
        for (field_names) |f| {
            if (f.val != null) {
                if (!first) try fw.writeByte(',');
                try fw.writeByte('"');
                try fw.writeAll(f.name);
                try fw.writeByte('"');
                first = false;
            }
        }

        return fetchAndReturnContact(allocator, db, id, "updated", fields_buf.items);
    }

    fn createContact(allocator: std.mem.Allocator, db_inst: *CrmDb, db: *c.sqlite3, name: []const u8, company_id: ?[]const u8, role: ?[]const u8, email: ?[]const u8, phone: ?[]const u8, notes: ?[]const u8) !root.ToolResult {
        const uuid = db_inst.generateUuid();
        const now = helpers.nowIso8601();

        const sql =
            \\INSERT INTO contacts (id, name, company_id, role, email, phone, notes, created_at, updated_at)
            \\VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare insert statement");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, &uuid, uuid.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, name.ptr, @intCast(name.len), schema.SQLITE_STATIC);
        helpers.bindOptionalText(stmt, 3, company_id);
        helpers.bindOptionalText(stmt, 4, role);
        helpers.bindOptionalText(stmt, 5, email);
        helpers.bindOptionalText(stmt, 6, phone);
        helpers.bindOptionalText(stmt, 7, notes);
        _ = c.sqlite3_bind_text(stmt, 8, &now, now.len, schema.SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 9, &now, now.len, schema.SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_DONE) return root.ToolResult.fail("Failed to insert contact");

        return fetchAndReturnContact(allocator, db, &uuid, "created", null);
    }

    fn fetchAndReturnContact(allocator: std.mem.Allocator, db: *c.sqlite3, id: []const u8, status: []const u8, fields_updated: ?[]const u8) !root.ToolResult {
        const sql =
            \\SELECT c.id, c.name, c.company_id, co.name, c.role, c.email, c.phone, c.notes, c.created_at, c.updated_at
            \\FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
            \\WHERE c.id = ?1;
        ;
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to fetch contact");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), schema.SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) return root.ToolResult.fail("Contact not found after save");

        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.writeAll("{\"status\":\"");
        try w.writeAll(status);
        try w.writeAll("\",\"contact\":{");
        try helpers.writeJsonField(w, "id", stmt, 0, true);
        try helpers.writeJsonField(w, "name", stmt, 1, false);
        try helpers.writeNullableField(w, "company_id", stmt, 2);
        try helpers.writeNullableField(w, "company_name", stmt, 3);
        try helpers.writeNullableField(w, "role", stmt, 4);
        try helpers.writeNullableField(w, "email", stmt, 5);
        try helpers.writeNullableField(w, "phone", stmt, 6);
        try helpers.writeNullableField(w, "notes", stmt, 7);
        try helpers.writeJsonField(w, "created_at", stmt, 8, false);
        try helpers.writeJsonField(w, "updated_at", stmt, 9, false);
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

// ── Tests ───────────────────────────────────────────────────────────

test "save_contact tool name" {
    var t = SaveContactTool{};
    const tool = t.tool();
    try std.testing.expectEqualStrings("save_contact", tool.name());
}

test "save_contact missing name" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveContactTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"email\":\"test@example.com\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "save_contact create new" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveContactTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"James Chen\",\"role\":\"VP Engineering\",\"email\":\"james@northstar.io\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "James Chen") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "VP Engineering") != null);
}

test "save_contact duplicate detection" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO contacts (id, name, role, created_at, updated_at) VALUES ('ct1', 'James Chen', 'CTO', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");
    try db.execSql("INSERT INTO contacts (id, name, role, created_at, updated_at) VALUES ('ct2', 'James Chen', 'VP Eng', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveContactTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"James Chen\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "disambiguation_needed") != null);
}

test "save_contact update existing" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO contacts (id, name, role, created_at, updated_at) VALUES ('ct1', 'James Chen', 'CTO', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveContactTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"James Chen\",\"email\":\"james@new.com\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"updated\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "james@new.com") != null);
}

test "save_contact optional fields omitted" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    var t = SaveContactTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"Minimal Contact\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
}

test "save_contact with company name resolution" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    try db.execSql("INSERT INTO companies (id, name, created_at, updated_at) VALUES ('co1', 'Northstar Technologies', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');");

    var t = SaveContactTool{ .db = &db };
    const parsed = try root.parseTestArgs("{\"name\":\"James Chen\",\"company\":\"Northstar Technologies\"}");
    defer parsed.deinit();
    const result = try t.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"status\":\"created\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "co1") != null);
}
