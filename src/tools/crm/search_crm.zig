//! search_crm tool — search CRM records using structured filters or LIKE-based text search.

const std = @import("std");
const root = @import("../root.zig");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;
const helpers = @import("save_company.zig");

pub const SearchCrmTool = struct {
    db: ?*CrmDb = null,

    pub const tool_name = "search_crm";
    pub const tool_description = "Search CRM records by keyword and optional structured filters (type, stage, min_value, company). Returns matching contacts, companies, deals, and activities.";
    pub const tool_params =
        \\{"type":"object","properties":{"query":{"type":"string","description":"Search query (natural language or keywords)"},"filters":{"type":"object","description":"Structured filters to narrow results","properties":{"type":{"type":"string","enum":["contact","company","deal","activity"],"description":"Record type to search"},"stage":{"type":"string","description":"Deal stage filter"},"min_value":{"type":"number","description":"Minimum deal value"},"company":{"type":"string","description":"Company name filter"}}},"limit":{"type":"number","description":"Maximum results to return (default: 10)"}},"required":["query"]}
    ;

    pub const vtable = root.ToolVTable(@This());

    pub fn tool(self: *SearchCrmTool) root.Tool {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    pub fn execute(self: *SearchCrmTool, allocator: std.mem.Allocator, args: root.JsonObjectMap) !root.ToolResult {
        const query = root.getString(args, "query") orelse
            return root.ToolResult.fail("Missing required 'query' parameter");

        const crm_db = self.db orelse
            return root.ToolResult.fail("CRM database not configured");

        const db = crm_db.db orelse
            return root.ToolResult.fail("CRM database not open");

        const limit_raw = root.getInt(args, "limit");
        const limit: i64 = if (limit_raw) |l| (if (l > 0) l else 10) else 10;

        // Check for filters
        const filters_val = root.getValue(args, "filters");
        const has_filters = if (filters_val) |fv| switch (fv) {
            .object => true,
            else => false,
        } else false;

        var results: std.ArrayList(u8) = .empty;
        errdefer results.deinit(allocator);

        try results.appendSlice(allocator, "{\"results\":[");

        var count: usize = 0;

        if (has_filters) {
            const filters = filters_val.?.object;
            const type_filter = root.getString(filters, "type");
            const stage_filter = root.getString(filters, "stage");
            const company_filter = root.getString(filters, "company");
            const min_value = getNumber(filters, "min_value");

            // Structured path
            if (type_filter) |t| {
                if (std.mem.eql(u8, t, "deal")) {
                    count = try searchDeals(db, allocator, &results, query, stage_filter, min_value, company_filter, limit);
                } else if (std.mem.eql(u8, t, "contact")) {
                    count = try searchContacts(db, allocator, &results, query, company_filter, limit);
                } else if (std.mem.eql(u8, t, "company")) {
                    count = try searchCompanies(db, allocator, &results, query, limit);
                } else if (std.mem.eql(u8, t, "activity")) {
                    count = try searchActivities(db, allocator, &results, query, company_filter, limit);
                }
            } else {
                // No type filter — search all tables
                const remaining = limit;
                count += try searchDeals(db, allocator, &results, query, stage_filter, min_value, company_filter, remaining);
                if (count < @as(usize, @intCast(remaining))) {
                    count += try searchContacts(db, allocator, &results, query, company_filter, remaining - @as(i64, @intCast(count)));
                }
                if (count < @as(usize, @intCast(remaining))) {
                    count += try searchCompanies(db, allocator, &results, query, remaining - @as(i64, @intCast(count)));
                }
                if (count < @as(usize, @intCast(remaining))) {
                    count += try searchActivities(db, allocator, &results, query, company_filter, remaining - @as(i64, @intCast(count)));
                }
            }
        } else {
            // Semantic path (for now, LIKE search across all tables)
            const remaining = limit;
            count += try searchDeals(db, allocator, &results, query, null, null, null, remaining);
            if (count < @as(usize, @intCast(remaining))) {
                count += try searchContacts(db, allocator, &results, query, null, remaining - @as(i64, @intCast(count)));
            }
            if (count < @as(usize, @intCast(remaining))) {
                count += try searchCompanies(db, allocator, &results, query, remaining - @as(i64, @intCast(count)));
            }
            if (count < @as(usize, @intCast(remaining))) {
                count += try searchActivities(db, allocator, &results, query, null, remaining - @as(i64, @intCast(count)));
            }
        }

        const search_path: []const u8 = if (has_filters) "structured" else "semantic";
        const w = results.writer(allocator);
        try w.writeAll("],\"total\":");
        try std.fmt.format(w, "{d}", .{count});
        try w.writeAll(",\"search_path\":\"");
        try w.writeAll(search_path);
        try w.writeAll("\"}");

        return root.ToolResult{ .success = true, .output = try results.toOwnedSlice(allocator) };
    }

    fn getNumber(filters: root.JsonObjectMap, key: []const u8) ?f64 {
        const val = filters.get(key) orelse return null;
        return switch (val) {
            .integer => |i| @floatFromInt(i),
            .float => |f| f,
            else => null,
        };
    }

    fn searchDeals(db: *c.sqlite3, allocator: std.mem.Allocator, results: *std.ArrayList(u8), query: []const u8, stage: ?[]const u8, min_value: ?f64, company: ?[]const u8, limit: i64) !usize {
        // Build SQL dynamically based on filters
        var sql_buf: std.ArrayList(u8) = .empty;
        defer sql_buf.deinit(allocator);

        try sql_buf.appendSlice(allocator,
            \\SELECT d.id, d.title, d.stage, d.value, d.currency, co.name as company_name
            \\FROM deals d LEFT JOIN companies co ON d.company_id = co.id
            \\WHERE (d.title LIKE ?1 OR d.notes LIKE ?1)
        );

        if (stage != null) try sql_buf.appendSlice(allocator, " AND d.stage = ?2");
        if (min_value != null) try sql_buf.appendSlice(allocator, " AND d.value >= ?3");
        if (company != null) try sql_buf.appendSlice(allocator, " AND co.name LIKE ?4");
        try sql_buf.appendSlice(allocator, " ORDER BY d.value DESC LIMIT ?5;");
        try sql_buf.append(allocator, 0);

        const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql_z, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return 0;
        defer _ = c.sqlite3_finalize(stmt);

        // Bind query as LIKE pattern
        const like_query = try std.fmt.allocPrint(allocator, "%{s}%", .{query});
        defer allocator.free(like_query);
        _ = c.sqlite3_bind_text(stmt, 1, like_query.ptr, @intCast(like_query.len), SQLITE_STATIC);

        if (stage) |s| {
            _ = c.sqlite3_bind_text(stmt, 2, s.ptr, @intCast(s.len), SQLITE_STATIC);
        }
        if (min_value) |mv| {
            _ = c.sqlite3_bind_double(stmt, 3, mv);
        }
        if (company) |comp| {
            const like_comp = try std.fmt.allocPrint(allocator, "%{s}%", .{comp});
            defer allocator.free(like_comp);
            _ = c.sqlite3_bind_text(stmt, 4, like_comp.ptr, @intCast(like_comp.len), SQLITE_STATIC);
        }
        _ = c.sqlite3_bind_int64(stmt, 5, limit);

        var count: usize = 0;
        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0) try results.append(allocator, ',');

            const id = columnText(stmt.?, 0);
            const title = columnText(stmt.?, 1);
            const deal_stage = columnText(stmt.?, 2);
            const value = c.sqlite3_column_double(stmt.?, 3);
            const company_name = columnText(stmt.?, 5);

            const w = results.writer(allocator);
            try w.writeAll("{\"type\":\"deal\",\"id\":\"");
            try helpers.writeJsonEscaped(w, id);
            try w.writeAll("\",\"title\":\"");
            try helpers.writeJsonEscaped(w, title);
            try w.writeAll("\",\"summary\":\"Deal with ");
            try helpers.writeJsonEscaped(w, company_name);
            try w.writeAll(", ");
            try helpers.writeJsonEscaped(w, deal_stage);
            try std.fmt.format(w, " stage, ${d:.0}", .{value});
            try w.writeAll("\",\"stage\":\"");
            try helpers.writeJsonEscaped(w, deal_stage);
            try w.writeAll("\",\"value\":");
            try std.fmt.format(w, "{d:.2}", .{value});
            try w.writeAll(",\"company\":\"");
            try helpers.writeJsonEscaped(w, company_name);
            try w.writeAll("\"}");
            count += 1;
        }
        return count;
    }

    fn searchContacts(db: *c.sqlite3, allocator: std.mem.Allocator, results: *std.ArrayList(u8), query: []const u8, company: ?[]const u8, limit: i64) !usize {
        var sql_buf: std.ArrayList(u8) = .empty;
        defer sql_buf.deinit(allocator);

        try sql_buf.appendSlice(allocator,
            \\SELECT ct.id, ct.name, ct.role, co.name as company_name
            \\FROM contacts ct LEFT JOIN companies co ON ct.company_id = co.id
            \\WHERE (ct.name LIKE ?1 OR ct.role LIKE ?1 OR ct.notes LIKE ?1)
        );

        if (company != null) try sql_buf.appendSlice(allocator, " AND co.name LIKE ?2");
        try sql_buf.appendSlice(allocator, " ORDER BY ct.name LIMIT ?3;");
        try sql_buf.append(allocator, 0);

        const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql_z, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return 0;
        defer _ = c.sqlite3_finalize(stmt);

        const like_query = try std.fmt.allocPrint(allocator, "%{s}%", .{query});
        defer allocator.free(like_query);
        _ = c.sqlite3_bind_text(stmt, 1, like_query.ptr, @intCast(like_query.len), SQLITE_STATIC);

        if (company) |comp| {
            const like_comp = try std.fmt.allocPrint(allocator, "%{s}%", .{comp});
            defer allocator.free(like_comp);
            _ = c.sqlite3_bind_text(stmt, 2, like_comp.ptr, @intCast(like_comp.len), SQLITE_STATIC);
        }
        _ = c.sqlite3_bind_int64(stmt, 3, limit);

        var count: usize = 0;
        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0 or results.items.len > "{\"results\":[".len)
                try results.append(allocator, ',');

            const id = columnText(stmt.?, 0);
            const name = columnText(stmt.?, 1);
            const role = columnText(stmt.?, 2);
            const company_name = columnText(stmt.?, 3);

            const w = results.writer(allocator);
            try w.writeAll("{\"type\":\"contact\",\"id\":\"");
            try helpers.writeJsonEscaped(w, id);
            try w.writeAll("\",\"name\":\"");
            try helpers.writeJsonEscaped(w, name);
            try w.writeAll("\",\"summary\":\"");
            try helpers.writeJsonEscaped(w, role);
            try w.writeAll(" at ");
            try helpers.writeJsonEscaped(w, company_name);
            try w.writeAll("\",\"company\":\"");
            try helpers.writeJsonEscaped(w, company_name);
            try w.writeAll("\"}");
            count += 1;
        }
        return count;
    }

    fn searchCompanies(db: *c.sqlite3, allocator: std.mem.Allocator, results: *std.ArrayList(u8), query: []const u8, limit: i64) !usize {
        const sql =
            \\SELECT id, name, industry, size
            \\FROM companies
            \\WHERE (name LIKE ?1 OR industry LIKE ?1 OR notes LIKE ?1)
            \\ORDER BY name LIMIT ?2;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return 0;
        defer _ = c.sqlite3_finalize(stmt);

        const like_query = try std.fmt.allocPrint(allocator, "%{s}%", .{query});
        defer allocator.free(like_query);
        _ = c.sqlite3_bind_text(stmt, 1, like_query.ptr, @intCast(like_query.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, limit);

        var count: usize = 0;
        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0 or results.items.len > "{\"results\":[".len)
                try results.append(allocator, ',');

            const id = columnText(stmt.?, 0);
            const name = columnText(stmt.?, 1);
            const industry = columnText(stmt.?, 2);
            const size = columnText(stmt.?, 3);

            const w = results.writer(allocator);
            try w.writeAll("{\"type\":\"company\",\"id\":\"");
            try helpers.writeJsonEscaped(w, id);
            try w.writeAll("\",\"name\":\"");
            try helpers.writeJsonEscaped(w, name);
            try w.writeAll("\",\"summary\":\"");
            try helpers.writeJsonEscaped(w, industry);
            try w.writeAll(", ");
            try helpers.writeJsonEscaped(w, size);
            try w.writeAll("\",\"industry\":\"");
            try helpers.writeJsonEscaped(w, industry);
            try w.writeAll("\",\"size\":\"");
            try helpers.writeJsonEscaped(w, size);
            try w.writeAll("\"}");
            count += 1;
        }
        return count;
    }

    fn searchActivities(db: *c.sqlite3, allocator: std.mem.Allocator, results: *std.ArrayList(u8), query: []const u8, company: ?[]const u8, limit: i64) !usize {
        var sql_buf: std.ArrayList(u8) = .empty;
        defer sql_buf.deinit(allocator);

        try sql_buf.appendSlice(allocator,
            \\SELECT a.id, a.type, a.summary, a.date, co.name as company_name
            \\FROM activities a LEFT JOIN companies co ON a.company_id = co.id
            \\WHERE (a.summary LIKE ?1)
        );

        if (company != null) try sql_buf.appendSlice(allocator, " AND co.name LIKE ?2");
        try sql_buf.appendSlice(allocator, " ORDER BY a.date DESC LIMIT ?3;");
        try sql_buf.append(allocator, 0);

        const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db, sql_z, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return 0;
        defer _ = c.sqlite3_finalize(stmt);

        const like_query = try std.fmt.allocPrint(allocator, "%{s}%", .{query});
        defer allocator.free(like_query);
        _ = c.sqlite3_bind_text(stmt, 1, like_query.ptr, @intCast(like_query.len), SQLITE_STATIC);

        if (company) |comp| {
            const like_comp = try std.fmt.allocPrint(allocator, "%{s}%", .{comp});
            defer allocator.free(like_comp);
            _ = c.sqlite3_bind_text(stmt, 2, like_comp.ptr, @intCast(like_comp.len), SQLITE_STATIC);
        }
        _ = c.sqlite3_bind_int64(stmt, 3, limit);

        var count: usize = 0;
        while (c.sqlite3_step(stmt.?) == c.SQLITE_ROW) {
            if (count > 0 or results.items.len > "{\"results\":[".len)
                try results.append(allocator, ',');

            const id = columnText(stmt.?, 0);
            const atype = columnText(stmt.?, 1);
            const summary = columnText(stmt.?, 2);
            const date = columnText(stmt.?, 3);
            const company_name = columnText(stmt.?, 4);

            const w = results.writer(allocator);
            try w.writeAll("{\"type\":\"activity\",\"id\":\"");
            try helpers.writeJsonEscaped(w, id);
            try w.writeAll("\",\"activity_type\":\"");
            try helpers.writeJsonEscaped(w, atype);
            try w.writeAll("\",\"summary\":\"");
            try helpers.writeJsonEscaped(w, summary);
            try w.writeAll("\",\"date\":\"");
            try helpers.writeJsonEscaped(w, date);
            try w.writeAll("\",\"company\":\"");
            try helpers.writeJsonEscaped(w, company_name);
            try w.writeAll("\"}");
            count += 1;
        }
        return count;
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
        \\INSERT INTO activities (id, type, contact_id, deal_id, company_id, summary, date, created_at)
        \\VALUES ('act1', 'meeting', 'cont1', 'deal1', 'comp1', 'Discussed Northstar implementation', '2026-03-01T14:30:00Z', '2026-03-01T14:30:00Z');
    ;
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, inserts, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);
    if (rc != c.SQLITE_OK) return error.SqliteExecFailed;
}

test "search_crm tool name and params" {
    var t = SearchCrmTool{};
    const tool_inst = t.tool();
    try std.testing.expectEqualStrings("search_crm", tool_inst.name());
    try std.testing.expect(tool_inst.parametersJson().len > 0);
}

test "search_crm missing query" {
    var t = SearchCrmTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "search_crm no db" {
    var t = SearchCrmTool{};
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"test\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    try std.testing.expect(!result.success);
}

test "search_crm semantic path across all tables" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = SearchCrmTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"Northstar\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Northstar") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "semantic") != null);
}

test "search_crm structured path with type filter" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = SearchCrmTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"Northstar\",\"filters\":{\"type\":\"deal\"}}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "deal") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "structured") != null);
}

test "search_crm structured path with stage filter" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = SearchCrmTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"deal\",\"filters\":{\"type\":\"deal\",\"stage\":\"proposal\"}}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Northstar") != null);
    // Should NOT contain Acme (negotiation stage)
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Acme") == null);
}

test "search_crm structured path with min_value" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = SearchCrmTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"deal\",\"filters\":{\"type\":\"deal\",\"min_value\":100000}}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    // Only Acme deal is >= 100000
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Acme") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Northstar Platform") == null);
}

test "search_crm structured path with company filter" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = SearchCrmTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"deal\",\"filters\":{\"company\":\"Acme\"}}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "Acme") != null);
}

test "search_crm no results" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
    try insertTestData(db.db.?);

    var t = SearchCrmTool{ .db = &db };
    const tool_inst = t.tool();
    const parsed = try root.parseTestArgs("{\"query\":\"nonexistent_xyz\"}");
    defer parsed.deinit();
    const result = try tool_inst.execute(std.testing.allocator, parsed.value.object);
    defer std.testing.allocator.free(result.output);
    try std.testing.expect(result.success);
    try std.testing.expect(std.mem.indexOf(u8, result.output, "\"total\":0") != null);
}
