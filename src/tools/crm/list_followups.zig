//! list_followups tool — surface upcoming and overdue follow-up activities.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;

pub const ListFollowupsTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "list_followups";
    pub const tool_description = "List upcoming and overdue follow-up activities, sorted by urgency (overdue first, then by date). Optionally filter by rep and time window.";
    pub const tool_params =
        \\{"type":"object","properties":{"rep_id":{"type":"string","description":"Filter by sales rep identifier"},"days_ahead":{"type":"number","description":"How many days ahead to look (default: 7)"},"include_overdue":{"type":"boolean","description":"Include overdue follow-ups (default: true)"}},"required":[]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *ListFollowupsTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *ListFollowupsTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const crm_db = self.db orelse
            return root.ToolResult.fail("CRM database not configured");

        const db = crm_db.db orelse
            return root.ToolResult.fail("CRM database not open");

        const rep_id = root.getString(args, "rep_id");
        const days_ahead_raw = root.getInt(args, "days_ahead");
        const days_ahead: i64 = if (days_ahead_raw) |d| (if (d > 0) d else 7) else 7;
        const include_overdue = root.getBool(args, "include_overdue") orelse true;

        // Build query dynamically
        var sql_buf = std.ArrayList(u8).init(allocator);
        defer sql_buf.deinit();

        try sql_buf.appendSlice(allocator,
            \\SELECT a.id, a.type, a.summary, a.date, a.follow_up_date, a.follow_up_note,
            \\  a.rep_id, ct.name as contact_name, d.title as deal_title, co.name as company_name
            \\FROM activities a
            \\LEFT JOIN contacts ct ON a.contact_id = ct.id
            \\LEFT JOIN deals d ON a.deal_id = d.id
            \\LEFT JOIN companies co ON a.company_id = co.id
            \\WHERE a.follow_up_date IS NOT NULL
        );

        if (include_overdue) {
            // Include both overdue and upcoming within days_ahead
            try sql_buf.appendSlice(allocator, " AND a.follow_up_date <= date('now', '+' || ?1 || ' days')");
        } else {
            // Only upcoming (today and future), within days_ahead
            try sql_buf.appendSlice(allocator, " AND a.follow_up_date >= date('now') AND a.follow_up_date <= date('now', '+' || ?1 || ' days')");
        }

        if (rep_id != null) {
            try sql_buf.appendSlice(allocator, " AND a.rep_id = ?2");
        }

        try sql_buf.appendSlice(allocator,
            \\ ORDER BY
            \\  CASE WHEN a.follow_up_date < date('now') THEN 0 ELSE 1 END,
            \\  a.follow_up_date ASC;
        );
        try sql_buf.append(allocator, 0);

        const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql_z, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return root.ToolResult.fail("Failed to prepare followups query");
        defer _ = c.sqlite3_finalize(stmt);

        // Bind days_ahead as text for the concatenation
        const days_str = try std.fmt.allocPrintZ(allocator, "{d}", .{days_ahead});
        defer allocator.free(days_str);
        _ = c.sqlite3_bind_text(stmt, 1, days_str.ptr, @intCast(days_str.len - 1), SQLITE_STATIC);

        if (rep_id) |rid| {
            _ = c.sqlite3_bind_text(stmt, 2, rid.ptr, @intCast(rid.len), SQLITE_STATIC);
        }

        var result = std.ArrayList(u8).init(allocator);
        errdefer result.deinit();

        try result.appendSlice(allocator, "{\"followups\":[");

        var count: usize = 0;
        var overdue_count: usize = 0;

        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0) try result.append(allocator, ',');

            const a_id = columnText(stmt.?, 0);
            const a_type = columnText(stmt.?, 1);
            const a_summary = columnText(stmt.?, 2);
            const a_date = columnText(stmt.?, 3);
            const follow_up_date = columnText(stmt.?, 4);
            const follow_up_note = columnText(stmt.?, 5);
            const contact_name = columnText(stmt.?, 7);
            const deal_title = columnText(stmt.?, 8);
            const company_name = columnText(stmt.?, 9);

            // Determine urgency by checking follow_up_date against today
            // We use a separate query to check since we can't easily compare dates in Zig
            const urgency = try getUrgency(db, allocator, follow_up_date);

            if (std.mem.eql(u8, urgency, "overdue")) {
                overdue_count += 1;
            }

            const entry = try std.fmt.allocPrint(allocator,
                \\{{"urgency":"{s}","activity_id":"{s}","type":"{s}","follow_up_date":"{s}","follow_up_note":"{s}","contact_name":"{s}","deal_title":"{s}","company_name":"{s}","original_summary":"{s}","original_date":"{s}"}}
            , .{ urgency, a_id, a_type, follow_up_date, follow_up_note, contact_name, deal_title, company_name, a_summary, a_date });
            defer allocator.free(entry);
            try result.appendSlice(allocator, entry);
            count += 1;
        }

        const tail = try std.fmt.allocPrint(allocator, "],\"total\":{d},\"overdue_count\":{d}}}", .{ count, overdue_count });
        defer allocator.free(tail);
        try result.appendSlice(allocator, tail);

        return root.ToolResult{ .success = true, .output = try result.toOwnedSlice(allocator) };
    }

    fn getUrgency(db: *c.sqlite3, allocator: std.mem.Allocator, follow_up_date: []const u8) ![]const u8 {
        _ = allocator;
        // Use SQLite to compare dates
        const sql = "SELECT CASE WHEN ?1 < date('now') THEN 'overdue' WHEN ?1 = date('now') THEN 'today' ELSE 'upcoming' END;";
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return "upcoming";
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, follow_up_date.ptr, @intCast(follow_up_date.len), SQLITE_STATIC);

        if (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            const result_text = columnText(stmt.?, 0);
            if (std.mem.eql(u8, result_text, "overdue")) return "overdue";
            if (std.mem.eql(u8, result_text, "today")) return "today";
        }
        return "upcoming";
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
        \\INSERT INTO companies (id, name, industry, size, created_at, updated_at)
        \\VALUES ('comp2', 'Acme Corp', 'Manufacturing', 'enterprise', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO contacts (id, name, company_id, role, created_at, updated_at)
        \\VALUES ('cont1', 'James Chen', 'comp1', 'VP Engineering', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO contacts (id, name, company_id, role, created_at, updated_at)
        \\VALUES ('cont2', 'Sarah Kim', 'comp2', 'CTO', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO deals (id, title, company_id, contact_id, stage, value, currency, created_at, updated_at)
        \\VALUES ('deal1', 'Northstar Platform License', 'comp1', 'cont1', 'proposal', 45000.0, 'USD', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO deals (id, title, company_id, contact_id, stage, value, currency, created_at, updated_at)
        \\VALUES ('deal2', 'Acme Enterprise Deal', 'comp2', 'cont2', 'negotiation', 120000.0, 'USD', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, rep_id, summary, date, follow_up_date, follow_up_note, created_at)
        \\VALUES ('act1', 'meeting', 'cont1', 'deal1', 'comp1', 'rep1', 'Discussed implementation', '2026-03-01T14:30:00Z', '2020-01-01', 'Send SOW draft', '2026-03-01T14:30:00Z');
        \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, rep_id, summary, date, follow_up_date, follow_up_note, created_at)
        \\VALUES ('act2', 'call', 'cont2', 'deal2', 'comp2', 'rep1', 'Budget review call', '2026-03-03T10:00:00Z', '2099-03-07', 'Check budget approval', '2026-03-03T10:00:00Z');
        \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, rep_id, summary, date, follow_up_date, follow_up_note, created_at)
        \\VALUES ('act3', 'email', 'cont1', 'deal1', 'comp1', 'rep2', 'Sent proposal', '2026-03-04T09:00:00Z', '2099-06-15', 'Follow up on proposal', '2026-03-04T09:00:00Z');
        \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, summary, date, created_at)
        \\VALUES ('act4', 'note', 'cont2', 'deal2', 'comp2', 'Internal note', '2026-03-05T11:00:00Z', '2026-03-05T11:00:00Z');
    ;
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, inserts, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);
    if (rc != c.SQLITE_OK) return error.SqliteExecFailed;
}

test "list_followups tool name" {
    var t = ListFollowupsTool{};
    const tool_inst = t.tool();
    try std.testing.expectEqualStrings("list_followups", tool_inst.name());
}

test "list_followups no db" {
    var t = ListFollowupsTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "list_followups returns overdue and upcoming" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = ListFollowupsTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"days_ahead\":36500}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // Should have 3 follow-ups (act1 overdue, act2 and act3 upcoming)
    try std.testing.expect(std.mem.indexOf(u8, result.output, "followups") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "overdue") != null);
    // act4 has no follow_up_date, should not appear
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Internal note") == null);
}

test "list_followups filter by rep_id" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = ListFollowupsTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"rep_id\":\"rep2\",\"days_ahead\":36500}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // Only act3 belongs to rep2
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Sent proposal") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Budget review") == null);
}

test "list_followups exclude overdue" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = ListFollowupsTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"include_overdue\":false,\"days_ahead\":36500}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // act1 is overdue (2020-01-01), should be excluded
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Send SOW draft") == null);
}

test "list_followups empty result" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    // No activities inserted

    var t = ListFollowupsTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"total\":0") != null);
}

test "list_followups overdue items sort first" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = ListFollowupsTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"days_ahead\":36500}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // The overdue item should appear before upcoming items
    const overdue_pos = std.mem.indexOf(u8, result.output, "overdue");
    const upcoming_pos = std.mem.indexOf(u8, result.output, "upcoming");
    try std.testing.expect(overdue_pos != null);
    try std.testing.expect(upcoming_pos != null);
    try std.testing.expect(overdue_pos.? < upcoming_pos.?);
}
