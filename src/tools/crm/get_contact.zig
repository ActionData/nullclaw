//! get_contact tool — retrieve a full contact record with company and recent activities.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;

pub const GetContactTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "get_contact";
    pub const tool_description = "Look up a contact by name or ID. Returns the full contact record with associated company and recent activities.";
    pub const tool_params =
        \\{"type":"object","properties":{"name":{"type":"string","description":"Contact name to look up"},"id":{"type":"string","description":"Contact UUID (takes precedence)"}},"required":[]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *GetContactTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *GetContactTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const id = root.getString(args, "id");
        const name = root.getString(args, "name");

        if (id == null and name == null)
            return root.ToolResult.fail("At least one of 'name' or 'id' is required");

        const crm_db = self.db orelse
            return root.ToolResult.fail("CRM database not configured");

        const db = crm_db.db orelse
            return root.ToolResult.fail("CRM database not open");

        if (id) |contact_id| {
            return self.lookupById(allocator, db, contact_id);
        }

        // Name-based lookup
        return self.lookupByName(allocator, db, name.?);
    }

    fn lookupById(self: *GetContactTool, allocator: std.mem.Allocator, db: *c.sqlite3, contact_id: []const u8) !root.ToolResult {
        _ = self;
        const sql =
            \\SELECT ct.id, ct.name, ct.role, ct.email, ct.phone, ct.notes,
            \\  ct.created_at, ct.updated_at, co.id, co.name, co.industry, co.size
            \\FROM contacts ct LEFT JOIN companies co ON ct.company_id = co.id
            \\WHERE ct.id = ?1;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare contact query");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, contact_id.ptr, @intCast(contact_id.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) {
            const msg = try std.fmt.allocPrint(allocator, "{{\"error\":\"Contact not found\",\"id\":\"{s}\"}}", .{contact_id});
            return root.ToolResult{ .success = false, .output = msg };
        }

        return buildContactResult(allocator, db, stmt.?);
    }

    fn lookupByName(self: *GetContactTool, allocator: std.mem.Allocator, db: *c.sqlite3, name: []const u8) !root.ToolResult {
        _ = self;
        const sql =
            \\SELECT ct.id, ct.name, ct.role, ct.email, ct.phone, ct.notes,
            \\  ct.created_at, ct.updated_at, co.id, co.name, co.industry, co.size
            \\FROM contacts ct LEFT JOIN companies co ON ct.company_id = co.id
            \\WHERE ct.name LIKE ?1 COLLATE NOCASE;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare contact query");
        defer _ = c.sqlite3_finalize(stmt);

        const like_name = try std.fmt.allocPrintZ(allocator, "%{s}%", .{name});
        defer allocator.free(like_name);
        _ = c.sqlite3_bind_text(stmt, 1, like_name.ptr, @intCast(like_name.len - 1), SQLITE_STATIC);

        // Collect all matches
        var matches = std.ArrayList(ContactMatch).init(allocator);
        defer {
            for (matches.items) |m| {
                allocator.free(m.id);
                allocator.free(m.name);
                allocator.free(m.company);
                allocator.free(m.role);
            }
            matches.deinit();
        }

        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            try matches.append(allocator, .{
                .id = try allocator.dupe(u8, columnText(stmt.?, 0)),
                .name = try allocator.dupe(u8, columnText(stmt.?, 1)),
                .role = try allocator.dupe(u8, columnText(stmt.?, 2)),
                .company = try allocator.dupe(u8, columnText(stmt.?, 9)),
            });
        }

        if (matches.items.len == 0) {
            const msg = try std.fmt.allocPrint(allocator, "{{\"error\":\"No contact found matching '{s}'\"}}", .{name});
            return root.ToolResult{ .success = false, .output = msg };
        }

        if (matches.items.len > 1) {
            // Disambiguation
            var result = std.ArrayList(u8).init(allocator);
            errdefer result.deinit();

            const header = try std.fmt.allocPrint(allocator,
                \\{{"status":"disambiguation_needed","message":"Multiple contacts match '{s}'. Which one did you mean?","candidates":[
            , .{name});
            defer allocator.free(header);
            try result.appendSlice(allocator, header);

            for (matches.items, 0..) |m, i| {
                if (i > 0) try result.append(allocator, ',');
                const entry = try std.fmt.allocPrint(allocator,
                    \\{{"id":"{s}","name":"{s}","company":"{s}","role":"{s}"}}
                , .{ m.id, m.name, m.company, m.role });
                defer allocator.free(entry);
                try result.appendSlice(allocator, entry);
            }
            try result.appendSlice(allocator, "]}");
            return root.ToolResult{ .success = true, .output = try result.toOwnedSlice(allocator) };
        }

        // Exactly one match — re-query to build full result
        // Reset and reuse the ID from the match
        const match_id = matches.items[0].id;

        const sql2 =
            \\SELECT ct.id, ct.name, ct.role, ct.email, ct.phone, ct.notes,
            \\  ct.created_at, ct.updated_at, co.id, co.name, co.industry, co.size
            \\FROM contacts ct LEFT JOIN companies co ON ct.company_id = co.id
            \\WHERE ct.id = ?1;
        ;

        var stmt2: ?*c.sqlite3_stmt = null;
        rc = c.sqlite3_prepare_v2(db, sql2, -1, &stmt2, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare contact query");
        defer _ = c.sqlite3_finalize(stmt2);

        _ = c.sqlite3_bind_text(stmt2, 1, match_id.ptr, @intCast(match_id.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt2.?);
        if (rc != c.SQLITE_ROW) return root.ToolResult.fail("Contact not found after match");

        return buildContactResult(allocator, db, stmt2.?);
    }

    fn buildContactResult(allocator: std.mem.Allocator, db: *c.sqlite3, stmt: *c.sqlite3_stmt) !root.ToolResult {
        const ct_id = columnText(stmt, 0);
        const ct_name = columnText(stmt, 1);
        const ct_role = columnText(stmt, 2);
        const ct_email = columnText(stmt, 3);
        const ct_phone = columnText(stmt, 4);
        const ct_notes = columnText(stmt, 5);
        const ct_created = columnText(stmt, 6);
        const ct_updated = columnText(stmt, 7);
        const co_id = columnText(stmt, 8);
        const co_name = columnText(stmt, 9);
        const co_industry = columnText(stmt, 10);
        const co_size = columnText(stmt, 11);

        var result = std.ArrayList(u8).init(allocator);
        errdefer result.deinit();

        const contact_json = try std.fmt.allocPrint(allocator,
            \\{{"contact":{{"id":"{s}","name":"{s}","role":"{s}","email":"{s}","phone":"{s}","notes":"{s}","created_at":"{s}","updated_at":"{s}"}},"company":{{"id":"{s}","name":"{s}","industry":"{s}","size":"{s}"}},"recent_activities":[
        , .{ ct_id, ct_name, ct_role, ct_email, ct_phone, ct_notes, ct_created, ct_updated, co_id, co_name, co_industry, co_size });
        defer allocator.free(contact_json);
        try result.appendSlice(allocator, contact_json);

        // Fetch recent activities (last 5)
        const act_sql =
            \\SELECT id, type, summary, date, follow_up_date
            \\FROM activities WHERE contact_id = ?1
            \\ORDER BY date DESC LIMIT 5;
        ;

        var act_stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, act_sql, -1, &act_stmt, null);
        if (rc == c.SQLITE_OK) {
            defer _ = c.sqlite3_finalize(act_stmt);
            _ = c.sqlite3_bind_text(act_stmt, 1, ct_id.ptr, @intCast(ct_id.len), SQLITE_STATIC);

            var act_count: usize = 0;
            while (c.sqlite3_step(act_stmt.?) == c.SQLITE_ROW) {
                if (act_count > 0) try result.append(allocator, ',');

                const a_id = columnText(act_stmt.?, 0);
                const a_type = columnText(act_stmt.?, 1);
                const a_summary = columnText(act_stmt.?, 2);
                const a_date = columnText(act_stmt.?, 3);
                const a_followup = columnText(act_stmt.?, 4);

                const entry = try std.fmt.allocPrint(allocator,
                    \\{{"id":"{s}","type":"{s}","summary":"{s}","date":"{s}","follow_up_date":"{s}"}}
                , .{ a_id, a_type, a_summary, a_date, a_followup });
                defer allocator.free(entry);
                try result.appendSlice(allocator, entry);
                act_count += 1;
            }
        }

        try result.appendSlice(allocator, "]}");
        return root.ToolResult{ .success = true, .output = try result.toOwnedSlice(allocator) };
    }

    const ContactMatch = struct {
        id: []const u8,
        name: []const u8,
        role: []const u8,
        company: []const u8,
    };

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
        \\INSERT INTO contacts (id, name, company_id, role, email, phone, notes, created_at, updated_at)
        \\VALUES ('cont1', 'James Chen', 'comp1', 'VP Engineering', 'james@northstar.io', '555-1234', 'Met at CloudConf', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO contacts (id, name, company_id, role, email, phone, notes, created_at, updated_at)
        \\VALUES ('cont3', 'James Chen', 'comp1', 'Sales Manager', 'james2@northstar.io', '555-5678', 'Different James', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO activities (id, type, contact_id, company_id, summary, date, follow_up_date, created_at)
        \\VALUES ('act1', 'meeting', 'cont1', 'comp1', 'Discussed implementation', '2026-03-01T14:30:00Z', '2026-03-14', '2026-03-01T14:30:00Z');
        \\INSERT INTO activities (id, type, contact_id, company_id, summary, date, created_at)
        \\VALUES ('act2', 'call', 'cont1', 'comp1', 'Follow-up call', '2026-03-05T10:00:00Z', '2026-03-05T10:00:00Z');
    ;
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, inserts, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);
    if (rc != c.SQLITE_OK) return error.SqliteExecFailed;
}

test "get_contact tool name" {
    var t = GetContactTool{};
    const tool_inst = t.tool();
    try std.testing.expectEqualStrings("get_contact", tool_inst.name());
}

test "get_contact missing both params" {
    var t = GetContactTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "get_contact by id" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetContactTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"id\":\"cont1\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "James Chen") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Northstar") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "recent_activities") != null);
}

test "get_contact by name single match" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    // Insert only one James
    const inserts: [:0]const u8 =
        \\INSERT INTO companies (id, name, industry, size, created_at, updated_at)
        \\VALUES ('comp1', 'Northstar Technologies', 'SaaS', 'mid-market', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO contacts (id, name, company_id, role, email, created_at, updated_at)
        \\VALUES ('cont1', 'James Chen', 'comp1', 'VP Engineering', 'james@northstar.io', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
    ;
    var err_msg: [*c]u8 = null;
    _ = c.sqlite3_exec(db.db, inserts, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);

    var t = GetContactTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"name\":\"James\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "James Chen") != null);
}

test "get_contact by name disambiguation" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetContactTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"name\":\"James\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "disambiguation_needed") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "candidates") != null);
}

test "get_contact not found" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetContactTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"id\":\"nonexistent\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "not found") != null);
}
