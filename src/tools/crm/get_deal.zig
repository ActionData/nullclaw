//! get_deal tool — retrieve a full deal record with company, contact, activities, and stage history.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;

pub const GetDealTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "get_deal";
    pub const tool_description = "Look up a deal by title, company, or ID. Returns the full deal record with contact, company, recent activities, and stage history.";
    pub const tool_params =
        \\{"type":"object","properties":{"title":{"type":"string","description":"Deal title to look up"},"company":{"type":"string","description":"Company name (returns all deals for that company)"},"id":{"type":"string","description":"Deal UUID (takes precedence)"}},"required":[]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *GetDealTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *GetDealTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const id = root.getString(args, "id");
        const title = root.getString(args, "title");
        const company = root.getString(args, "company");

        if (id == null and title == null and company == null)
            return root.ToolResult.fail("At least one of 'title', 'company', or 'id' is required");

        const crm_db = self.db orelse
            return root.ToolResult.fail("CRM database not configured");

        const db = crm_db.db orelse
            return root.ToolResult.fail("CRM database not open");

        // Resolution priority: id > title > company
        if (id) |deal_id| {
            return self.lookupById(allocator, db, deal_id);
        }

        if (title) |deal_title| {
            return self.lookupByTitle(allocator, db, deal_title);
        }

        // Company-only: return all deals for that company
        return self.lookupByCompany(allocator, db, company.?);
    }

    fn lookupById(self: *GetDealTool, allocator: std.mem.Allocator, db: *c.sqlite3, deal_id: []const u8) !root.ToolResult {
        _ = self;
        const sql =
            \\SELECT d.id, d.title, d.stage, d.value, d.currency, d.close_date,
            \\  d.next_step, d.notes, d.created_at, d.updated_at,
            \\  co.id, co.name, ct.id, ct.name, ct.role
            \\FROM deals d
            \\LEFT JOIN companies co ON d.company_id = co.id
            \\LEFT JOIN contacts ct ON d.contact_id = ct.id
            \\WHERE d.id = ?1;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare deal query");
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, deal_id.ptr, @intCast(deal_id.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) {
            const msg = try std.fmt.allocPrint(allocator, "{{\"error\":\"Deal not found\",\"id\":\"{s}\"}}", .{deal_id});
            return root.ToolResult{ .success = false, .output = msg };
        }

        return buildDealResult(allocator, db, stmt.?);
    }

    fn lookupByTitle(self: *GetDealTool, allocator: std.mem.Allocator, db: *c.sqlite3, title: []const u8) !root.ToolResult {
        _ = self;
        const sql =
            \\SELECT d.id, d.title, d.stage, d.value, d.currency, d.close_date,
            \\  d.next_step, d.notes, d.created_at, d.updated_at,
            \\  co.id, co.name, ct.id, ct.name, ct.role
            \\FROM deals d
            \\LEFT JOIN companies co ON d.company_id = co.id
            \\LEFT JOIN contacts ct ON d.contact_id = ct.id
            \\WHERE d.title LIKE ?1 COLLATE NOCASE;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare deal query");
        defer _ = c.sqlite3_finalize(stmt);

        const like_title = try std.fmt.allocPrint(allocator, "%{s}%", .{title});
        defer allocator.free(like_title);
        _ = c.sqlite3_bind_text(stmt, 1, like_title.ptr, @intCast(like_title.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) {
            const msg = try std.fmt.allocPrint(allocator, "{{\"error\":\"No deal found matching '{s}'\"}}", .{title});
            return root.ToolResult{ .success = false, .output = msg };
        }

        return buildDealResult(allocator, db, stmt.?);
    }

    fn lookupByCompany(self: *GetDealTool, allocator: std.mem.Allocator, db: *c.sqlite3, company: []const u8) !root.ToolResult {
        _ = self;
        const sql =
            \\SELECT d.id, d.title, d.stage, d.value, d.currency, d.close_date,
            \\  d.next_step, d.notes, d.created_at, d.updated_at,
            \\  co.id, co.name, ct.id, ct.name, ct.role
            \\FROM deals d
            \\LEFT JOIN companies co ON d.company_id = co.id
            \\LEFT JOIN contacts ct ON d.contact_id = ct.id
            \\WHERE co.name LIKE ?1 COLLATE NOCASE;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare deal query");
        defer _ = c.sqlite3_finalize(stmt);

        const like_company = try std.fmt.allocPrint(allocator, "%{s}%", .{company});
        defer allocator.free(like_company);
        _ = c.sqlite3_bind_text(stmt, 1, like_company.ptr, @intCast(like_company.len), SQLITE_STATIC);

        // Collect all deals for this company
        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(allocator);

        try result.appendSlice(allocator, "{\"deals\":[");

        var count: usize = 0;
        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0) try result.append(allocator, ',');

            const d_id = columnText(stmt.?, 0);
            const d_title = columnText(stmt.?, 1);
            const d_stage = columnText(stmt.?, 2);
            const d_value = c.sqlite3_column_double(stmt.?, 3);
            const d_currency = columnText(stmt.?, 4);
            const co_name = columnText(stmt.?, 11);
            const ct_name = columnText(stmt.?, 13);

            const entry = try std.fmt.allocPrint(allocator,
                \\{{"id":"{s}","title":"{s}","stage":"{s}","value":{d:.2},"currency":"{s}","company":"{s}","contact":"{s}"}}
            , .{ d_id, d_title, d_stage, d_value, d_currency, co_name, ct_name });
            defer allocator.free(entry);
            try result.appendSlice(allocator, entry);
            count += 1;
        }

        if (count == 0) {
            result.deinit(allocator);
            const msg = try std.fmt.allocPrint(allocator, "{{\"error\":\"No deals found for company '{s}'\"}}", .{company});
            return root.ToolResult{ .success = false, .output = msg };
        }

        const tail = try std.fmt.allocPrint(allocator, "],\"total\":{d},\"company\":\"{s}\"}}", .{ count, company });
        defer allocator.free(tail);
        try result.appendSlice(allocator, tail);

        return root.ToolResult{ .success = true, .output = try result.toOwnedSlice(allocator) };
    }

    fn buildDealResult(allocator: std.mem.Allocator, db: *c.sqlite3, stmt: *c.sqlite3_stmt) !root.ToolResult {
        const d_id = columnText(stmt, 0);
        const d_title = columnText(stmt, 1);
        const d_stage = columnText(stmt, 2);
        const d_value = c.sqlite3_column_double(stmt, 3);
        const d_currency = columnText(stmt, 4);
        const d_close_date = columnText(stmt, 5);
        const d_next_step = columnText(stmt, 6);
        const d_notes = columnText(stmt, 7);
        const d_created = columnText(stmt, 8);
        const d_updated = columnText(stmt, 9);
        const co_id = columnText(stmt, 10);
        const co_name = columnText(stmt, 11);
        const ct_id = columnText(stmt, 12);
        const ct_name = columnText(stmt, 13);
        const ct_role = columnText(stmt, 14);

        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(allocator);

        const deal_json = try std.fmt.allocPrint(allocator,
            \\{{"deal":{{"id":"{s}","title":"{s}","stage":"{s}","value":{d:.2},"currency":"{s}","close_date":"{s}","next_step":"{s}","notes":"{s}","created_at":"{s}","updated_at":"{s}"}},"company":{{"id":"{s}","name":"{s}"}},"contact":{{"id":"{s}","name":"{s}","role":"{s}"}},"recent_activities":[
        , .{ d_id, d_title, d_stage, d_value, d_currency, d_close_date, d_next_step, d_notes, d_created, d_updated, co_id, co_name, ct_id, ct_name, ct_role });
        defer allocator.free(deal_json);
        try result.appendSlice(allocator, deal_json);

        // Fetch recent activities (last 5)
        const act_sql =
            \\SELECT id, type, summary, date, follow_up_date
            \\FROM activities WHERE deal_id = ?1
            \\ORDER BY date DESC LIMIT 5;
        ;

        var act_stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, act_sql, -1, &act_stmt, null);
        if (rc == c.SQLITE_OK) {
            defer _ = c.sqlite3_finalize(act_stmt);
            _ = c.sqlite3_bind_text(act_stmt, 1, d_id.ptr, @intCast(d_id.len), SQLITE_STATIC);

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

        try result.appendSlice(allocator, "],\"stage_history\":[");

        // Fetch stage history
        const hist_sql =
            \\SELECT from_stage, to_stage, notes, changed_at
            \\FROM stage_history WHERE deal_id = ?1
            \\ORDER BY changed_at ASC;
        ;

        var hist_stmt: ?*c.sqlite3_stmt = null;
        rc = c.sqlite3_prepare_v2(db, hist_sql, -1, &hist_stmt, null);
        if (rc == c.SQLITE_OK) {
            defer _ = c.sqlite3_finalize(hist_stmt);
            _ = c.sqlite3_bind_text(hist_stmt, 1, d_id.ptr, @intCast(d_id.len), SQLITE_STATIC);

            var hist_count: usize = 0;
            while (c.sqlite3_step(hist_stmt.?) == c.SQLITE_ROW) {
                if (hist_count > 0) try result.append(allocator, ',');

                const from = columnText(hist_stmt.?, 0);
                const to = columnText(hist_stmt.?, 1);
                const notes = columnText(hist_stmt.?, 2);
                const changed = columnText(hist_stmt.?, 3);

                const entry = try std.fmt.allocPrint(allocator,
                    \\{{"from_stage":"{s}","to_stage":"{s}","notes":"{s}","changed_at":"{s}"}}
                , .{ from, to, notes, changed });
                defer allocator.free(entry);
                try result.appendSlice(allocator, entry);
                hist_count += 1;
            }
        }

        try result.appendSlice(allocator, "]}");
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
        \\INSERT INTO contacts (id, name, company_id, role, created_at, updated_at)
        \\VALUES ('cont1', 'James Chen', 'comp1', 'VP Engineering', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO deals (id, title, company_id, contact_id, stage, value, currency, close_date, next_step, notes, created_at, updated_at)
        \\VALUES ('deal1', 'Northstar Platform License', 'comp1', 'cont1', 'proposal', 45000.0, 'USD', '2026-06-30', 'Send SOW', 'Multi-year potential', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO deals (id, title, company_id, contact_id, stage, value, currency, created_at, updated_at)
        \\VALUES ('deal2', 'Northstar Support Contract', 'comp1', 'cont1', 'lead', 5000.0, 'USD', '2026-02-01T00:00:00Z', '2026-02-01T00:00:00Z');
        \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, summary, date, created_at)
        \\VALUES ('act1', 'meeting', 'cont1', 'deal1', 'comp1', 'Discussed implementation', '2026-03-01T14:30:00Z', '2026-03-01T14:30:00Z');
        \\INSERT INTO stage_history (id, deal_id, from_stage, to_stage, notes, changed_at)
        \\VALUES ('sh1', 'deal1', '', 'lead', 'Initial creation', '2026-01-01T00:00:00Z');
        \\INSERT INTO stage_history (id, deal_id, from_stage, to_stage, notes, changed_at)
        \\VALUES ('sh2', 'deal1', 'lead', 'proposal', 'Qualified', '2026-02-15T00:00:00Z');
    ;
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, inserts, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);
    if (rc != c.SQLITE_OK) return error.SqliteExecFailed;
}

test "get_deal tool name" {
    var t = GetDealTool{};
    const tool_inst = t.tool();
    try std.testing.expectEqualStrings("get_deal", tool_inst.name());
}

test "get_deal missing all params" {
    var t = GetDealTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "get_deal by id" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetDealTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"id\":\"deal1\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Northstar Platform License") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "stage_history") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "recent_activities") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "James Chen") != null);
}

test "get_deal by title" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetDealTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"title\":\"Platform License\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Northstar Platform License") != null);
}

test "get_deal by company returns all deals" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetDealTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"company\":\"Northstar\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"total\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Platform License") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Support Contract") != null);
}

test "get_deal not found" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetDealTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"id\":\"nonexistent\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(!result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "not found") != null);
}

test "get_deal includes stage history" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = GetDealTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"id\":\"deal1\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // Should have two stage history entries
    try std.testing.expect(std.mem.indexOf(u8, result.output, "lead") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "proposal") != null);
}
