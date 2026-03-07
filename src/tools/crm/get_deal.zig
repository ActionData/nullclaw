//! get_deal tool — retrieve a full deal record with company, contact, activities, and stage history.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;
const resolve = @import("resolve.zig");
const helpers = @import("save_company.zig");

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
        if (id != null or title != null) {
            var result = try resolve.resolveDeal(crm_db, allocator, title, id, null);
            defer resolve.freeResult(allocator, &result);

            switch (result) {
                .resolved => |r| {
                    return self.lookupById(allocator, db, r.id);
                },
                .ambiguous => |candidates| {
                    const msg = try resolve.formatCandidates(allocator, candidates, "deal");
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
                    try w.writeAll("{\"error\":\"Deal not found\",\"query\":\"");
                    if (id) |the_id| {
                        try helpers.writeJsonEscaped(w, the_id);
                    } else {
                        try helpers.writeJsonEscaped(w, title.?);
                    }
                    try w.writeAll("\"}");
                    return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
                },
            }
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
            var buf: std.ArrayList(u8) = .empty;
            errdefer buf.deinit(allocator);
            const w = buf.writer(allocator);
            try w.writeAll("{\"error\":\"Deal not found\",\"id\":\"");
            try helpers.writeJsonEscaped(w, deal_id);
            try w.writeAll("\"}");
            return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
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
        const w = result.writer(allocator);

        try w.writeAll("{\"deals\":[");

        var count: usize = 0;
        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0) try w.writeByte(',');

            const d_id = columnText(stmt.?, 0);
            const d_title = columnText(stmt.?, 1);
            const d_stage = columnText(stmt.?, 2);
            const d_value = c.sqlite3_column_double(stmt.?, 3);
            const d_currency = columnText(stmt.?, 4);
            const co_name = columnText(stmt.?, 11);
            const ct_name = columnText(stmt.?, 13);

            try w.writeAll("{\"id\":\"");
            try helpers.writeJsonEscaped(w, d_id);
            try w.writeAll("\",\"title\":\"");
            try helpers.writeJsonEscaped(w, d_title);
            try w.writeAll("\",\"stage\":\"");
            try helpers.writeJsonEscaped(w, d_stage);
            try w.writeAll("\",\"value\":");
            try std.fmt.format(w, "{d:.2}", .{d_value});
            try w.writeAll(",\"currency\":\"");
            try helpers.writeJsonEscaped(w, d_currency);
            try w.writeAll("\",\"company\":\"");
            try helpers.writeJsonEscaped(w, co_name);
            try w.writeAll("\",\"contact\":\"");
            try helpers.writeJsonEscaped(w, ct_name);
            try w.writeAll("\"}");
            count += 1;
        }

        if (count == 0) {
            result.deinit(allocator);
            var buf: std.ArrayList(u8) = .empty;
            errdefer buf.deinit(allocator);
            const bw = buf.writer(allocator);
            try bw.writeAll("{\"error\":\"No deals found for company '");
            try helpers.writeJsonEscaped(bw, company);
            try bw.writeAll("'\"}");
            return root.ToolResult{ .success = false, .output = try buf.toOwnedSlice(allocator) };
        }

        try w.writeAll("],\"total\":");
        try std.fmt.format(w, "{d}", .{count});
        try w.writeAll(",\"company\":\"");
        try helpers.writeJsonEscaped(w, company);
        try w.writeAll("\"}");

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
        const w = result.writer(allocator);

        try w.writeAll("{\"deal\":{\"id\":\"");
        try helpers.writeJsonEscaped(w, d_id);
        try w.writeAll("\",\"title\":\"");
        try helpers.writeJsonEscaped(w, d_title);
        try w.writeAll("\",\"stage\":\"");
        try helpers.writeJsonEscaped(w, d_stage);
        try w.writeAll("\",\"value\":");
        try std.fmt.format(w, "{d:.2}", .{d_value});
        try w.writeAll(",\"currency\":\"");
        try helpers.writeJsonEscaped(w, d_currency);
        try w.writeAll("\",\"close_date\":\"");
        try helpers.writeJsonEscaped(w, d_close_date);
        try w.writeAll("\",\"next_step\":\"");
        try helpers.writeJsonEscaped(w, d_next_step);
        try w.writeAll("\",\"notes\":\"");
        try helpers.writeJsonEscaped(w, d_notes);
        try w.writeAll("\",\"created_at\":\"");
        try helpers.writeJsonEscaped(w, d_created);
        try w.writeAll("\",\"updated_at\":\"");
        try helpers.writeJsonEscaped(w, d_updated);
        try w.writeAll("\"},\"company\":{\"id\":\"");
        try helpers.writeJsonEscaped(w, co_id);
        try w.writeAll("\",\"name\":\"");
        try helpers.writeJsonEscaped(w, co_name);
        try w.writeAll("\"},\"contact\":{\"id\":\"");
        try helpers.writeJsonEscaped(w, ct_id);
        try w.writeAll("\",\"name\":\"");
        try helpers.writeJsonEscaped(w, ct_name);
        try w.writeAll("\",\"role\":\"");
        try helpers.writeJsonEscaped(w, ct_role);
        try w.writeAll("\"},\"recent_activities\":[");

        // Fetch recent activities (last 10)
        const act_sql =
            \\SELECT id, type, summary, date, follow_up_date
            \\FROM activities WHERE deal_id = ?1
            \\ORDER BY date DESC LIMIT 10;
        ;

        var act_stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db, act_sql, -1, &act_stmt, null);
        if (rc == c.SQLITE_OK) {
            defer _ = c.sqlite3_finalize(act_stmt);
            _ = c.sqlite3_bind_text(act_stmt, 1, d_id.ptr, @intCast(d_id.len), SQLITE_STATIC);

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

        try w.writeAll("],\"stage_history\":[");

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
                if (hist_count > 0) try w.writeByte(',');

                const from = columnText(hist_stmt.?, 0);
                const to = columnText(hist_stmt.?, 1);
                const hist_notes = columnText(hist_stmt.?, 2);
                const changed = columnText(hist_stmt.?, 3);

                try w.writeAll("{\"from_stage\":\"");
                try helpers.writeJsonEscaped(w, from);
                try w.writeAll("\",\"to_stage\":\"");
                try helpers.writeJsonEscaped(w, to);
                try w.writeAll("\",\"notes\":\"");
                try helpers.writeJsonEscaped(w, hist_notes);
                try w.writeAll("\",\"changed_at\":\"");
                try helpers.writeJsonEscaped(w, changed);
                try w.writeAll("\"}");
                hist_count += 1;
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
