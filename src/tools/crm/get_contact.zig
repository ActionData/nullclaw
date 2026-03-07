//! get_contact tool — retrieve a full contact record with company and recent activities.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;
const resolve = @import("resolve.zig");
const helpers = @import("save_company.zig");

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

        // Use resolve module for both ID and name lookups
        var result = try resolve.resolveContact(crm_db, allocator, name, id, null);
        defer resolve.freeResult(allocator, &result);

        switch (result) {
            .resolved => |r| {
                return self.lookupById(allocator, db, r.id);
            },
            .ambiguous => |candidates| {
                const msg = try resolve.formatCandidates(allocator, candidates, "contact");
                defer allocator.free(msg);
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const w = buf.writer(allocator);
                try w.writeAll("{\"status\":\"disambiguation_needed\",\"message\":\"");
                try helpers.writeJsonEscaped(w, msg);
                try w.writeAll("\"}");
                return root.ToolResult{ .success = true, .output = try buf.toOwnedSlice(allocator) };
            },
            .not_found => {
                var buf: std.ArrayList(u8) = .empty;
                errdefer buf.deinit(allocator);
                const w = buf.writer(allocator);
                try w.writeAll("{\"error\":\"Contact not found\",\"query\":\"");
                if (id) |the_id| {
                    try helpers.writeJsonEscaped(w, the_id);
                } else {
                    try helpers.writeJsonEscaped(w, name.?);
                }
                try w.writeAll("\"}");
                return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
            },
        }
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
            var buf: std.ArrayList(u8) = .empty;
            errdefer buf.deinit(allocator);
            const w = buf.writer(allocator);
            try w.writeAll("{\"error\":\"Contact not found\",\"id\":\"");
            try helpers.writeJsonEscaped(w, contact_id);
            try w.writeAll("\"}");
            return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
        }

        return buildContactResult(allocator, db, stmt.?);
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

        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(allocator);
        const w = result.writer(allocator);

        try w.writeAll("{\"contact\":{\"id\":\"");
        try helpers.writeJsonEscaped(w, ct_id);
        try w.writeAll("\",\"name\":\"");
        try helpers.writeJsonEscaped(w, ct_name);
        try w.writeAll("\",\"role\":\"");
        try helpers.writeJsonEscaped(w, ct_role);
        try w.writeAll("\",\"email\":\"");
        try helpers.writeJsonEscaped(w, ct_email);
        try w.writeAll("\",\"phone\":\"");
        try helpers.writeJsonEscaped(w, ct_phone);
        try w.writeAll("\",\"notes\":\"");
        try helpers.writeJsonEscaped(w, ct_notes);
        try w.writeAll("\",\"created_at\":\"");
        try helpers.writeJsonEscaped(w, ct_created);
        try w.writeAll("\",\"updated_at\":\"");
        try helpers.writeJsonEscaped(w, ct_updated);
        try w.writeAll("\"},\"company\":{\"id\":\"");
        try helpers.writeJsonEscaped(w, co_id);
        try w.writeAll("\",\"name\":\"");
        try helpers.writeJsonEscaped(w, co_name);
        try w.writeAll("\",\"industry\":\"");
        try helpers.writeJsonEscaped(w, co_industry);
        try w.writeAll("\",\"size\":\"");
        try helpers.writeJsonEscaped(w, co_size);
        try w.writeAll("\"},\"recent_activities\":[");

        // Fetch recent activities (last 10)
        const act_sql =
            \\SELECT id, type, summary, date, follow_up_date
            \\FROM activities WHERE contact_id = ?1
            \\ORDER BY date DESC LIMIT 10;
        ;

        var act_stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, act_sql, -1, &act_stmt, null);
        if (rc == c.SQLITE_OK) {
            defer _ = c.sqlite3_finalize(act_stmt);
            _ = c.sqlite3_bind_text(act_stmt, 1, ct_id.ptr, @intCast(ct_id.len), SQLITE_STATIC);

            var act_count: usize = 0;
            while (c.sqlite3_step(act_stmt.?) == c.SQLITE_ROW) {
                if (act_count > 0) try w.writeByte(',');

                const a_id = columnText(act_stmt.?, 0);
                const a_type = columnText(act_stmt.?, 1);
                const a_summary = columnText(act_stmt.?, 2);
                const a_date = columnText(act_stmt.?, 3);
                const a_followup = columnText(act_stmt.?, 4);

                try w.writeAll("{\"id\":\"");
                try helpers.writeJsonEscaped(w, a_id);
                try w.writeAll("\",\"type\":\"");
                try helpers.writeJsonEscaped(w, a_type);
                try w.writeAll("\",\"summary\":\"");
                try helpers.writeJsonEscaped(w, a_summary);
                try w.writeAll("\",\"date\":\"");
                try helpers.writeJsonEscaped(w, a_date);
                try w.writeAll("\",\"follow_up_date\":\"");
                try helpers.writeJsonEscaped(w, a_followup);
                try w.writeAll("\"}");
                act_count += 1;
            }
        }

        try w.writeAll("]}");
        return root.ToolResult{ .success = true, .output = try result.toOwnedSlice(allocator) };
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
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Which one did you mean?") != null);
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
