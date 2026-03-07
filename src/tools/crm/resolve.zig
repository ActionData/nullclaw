//! CRM name resolution module — shared logic for looking up records by name,
//! fuzzy matching, and disambiguation.
//!
//! Used by all CRM tools to translate user-friendly names into database
//! record IDs. Supports tiered matching (exact, prefix, contains) with
//! scoring, and returns structured results that allow the agent to
//! disambiguate with the user when multiple candidates match.

const std = @import("std");
const schema = @import("schema.zig");
const CrmDb = schema.CrmDb;
const log = std.log.scoped(.crm_resolve);

const c = schema.c;
const SQLITE_STATIC = schema.SQLITE_STATIC;

// ── Types ──────────────────────────────────────────────────────────

pub const MatchType = enum {
    exact,
    prefix,
    contains,
    token_overlap,
};

pub const MatchCandidate = struct {
    id: []const u8,
    name: []const u8,
    match_type: MatchType,
    score: f32,
    context: []const u8,
};

pub const ResolveResult = union(enum) {
    resolved: struct {
        id: []const u8,
        name: []const u8,
    },
    ambiguous: []MatchCandidate,
    not_found,
};

// ── Score constants ────────────────────────────────────────────────

const SCORE_EXACT: f32 = 1.0;
const SCORE_PREFIX: f32 = 0.7;
const SCORE_CONTAINS: f32 = 0.5;

// ── Public resolve functions ───────────────────────────────────────

/// Resolve a company by name or ID.
pub fn resolveCompany(
    db: *CrmDb,
    allocator: std.mem.Allocator,
    name: ?[]const u8,
    id: ?[]const u8,
) !ResolveResult {
    // ID-based lookup
    if (id) |the_id| {
        return resolveById(db, allocator, "companies", "name", the_id, formatCompanyContext);
    }

    // Name-based lookup
    if (name) |the_name| {
        return resolveByName(db, allocator, .{
            .table = "companies",
            .name_col = "name",
            .query = the_name,
            .company_id = null,
            .format_context = formatCompanyContext,
        });
    }

    return .not_found;
}

/// Resolve a contact by name or ID, optionally scoped to a company.
pub fn resolveContact(
    db: *CrmDb,
    allocator: std.mem.Allocator,
    name: ?[]const u8,
    id: ?[]const u8,
    company_id: ?[]const u8,
) !ResolveResult {
    // ID-based lookup
    if (id) |the_id| {
        return resolveById(db, allocator, "contacts", "name", the_id, formatContactContext);
    }

    // Name-based lookup
    if (name) |the_name| {
        return resolveByName(db, allocator, .{
            .table = "contacts",
            .name_col = "name",
            .query = the_name,
            .company_id = company_id,
            .format_context = formatContactContext,
        });
    }

    return .not_found;
}

/// Resolve a deal by title or ID, optionally scoped to a company.
pub fn resolveDeal(
    db: *CrmDb,
    allocator: std.mem.Allocator,
    title: ?[]const u8,
    id: ?[]const u8,
    company_id: ?[]const u8,
) !ResolveResult {
    // ID-based lookup
    if (id) |the_id| {
        return resolveById(db, allocator, "deals", "title", the_id, formatDealContext);
    }

    // Name-based lookup
    if (title) |the_title| {
        return resolveByName(db, allocator, .{
            .table = "deals",
            .name_col = "title",
            .query = the_title,
            .company_id = company_id,
            .format_context = formatDealContext,
        });
    }

    return .not_found;
}

/// Format a list of disambiguation candidates into a human-readable string.
pub fn formatCandidates(
    allocator: std.mem.Allocator,
    candidates: []MatchCandidate,
    entity_type: []const u8,
) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    const writer = buf.writer();
    try writer.print("Multiple {s}s found:\n", .{entity_type});

    for (candidates, 1..) |cand, i| {
        if (cand.context.len > 0) {
            try writer.print("{d}. {s} ({s})\n", .{ i, cand.name, cand.context });
        } else {
            try writer.print("{d}. {s}\n", .{ i, cand.name });
        }
    }
    try writer.writeAll("Which one did you mean?");

    return buf.toOwnedSlice();
}

// ── Internal resolution helpers ────────────────────────────────────

const ContextFormatFn = *const fn (*CrmDb, std.mem.Allocator, []const u8) ?[]const u8;

const ResolveByNameOpts = struct {
    table: [:0]const u8,
    name_col: [:0]const u8,
    query: []const u8,
    company_id: ?[]const u8,
    format_context: ContextFormatFn,
};

fn resolveById(
    db: *CrmDb,
    allocator: std.mem.Allocator,
    table: [:0]const u8,
    name_col: [:0]const u8,
    id: []const u8,
    format_context: ContextFormatFn,
) !ResolveResult {
    _ = format_context;
    _ = allocator;

    // Build query: SELECT id, <name_col> FROM <table> WHERE id = ?
    var sql_buf: [256]u8 = undefined;
    const sql_len = (std.fmt.bufPrint(&sql_buf, "SELECT id, {s} FROM {s} WHERE id = ?1;", .{ name_col, table }) catch return error.SqlQueryTooLong).len;
    sql_buf[sql_len] = 0;
    const sql: [*:0]const u8 = sql_buf[0..sql_len :0];

    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    rc = c.sqlite3_step(stmt.?);

    if (rc == c.SQLITE_ROW) {
        const row_id = std.mem.span(c.sqlite3_column_text(stmt.?, 0));
        const row_name = std.mem.span(c.sqlite3_column_text(stmt.?, 1));
        return .{ .resolved = .{ .id = row_id, .name = row_name } };
    }

    return .not_found;
}

fn resolveByName(
    db: *CrmDb,
    allocator: std.mem.Allocator,
    opts: ResolveByNameOpts,
) !ResolveResult {
    // Try each tier in order: exact, prefix, contains
    const tiers = [_]struct { match_type: MatchType, score: f32, pattern_fmt: PatternFmt }{
        .{ .match_type = .exact, .score = SCORE_EXACT, .pattern_fmt = .exact },
        .{ .match_type = .prefix, .score = SCORE_PREFIX, .pattern_fmt = .prefix },
        .{ .match_type = .contains, .score = SCORE_CONTAINS, .pattern_fmt = .contains },
    };

    for (tiers) |tier| {
        const candidates = try queryByPattern(
            db,
            allocator,
            opts.table,
            opts.name_col,
            opts.query,
            tier.pattern_fmt,
            tier.match_type,
            tier.score,
            opts.company_id,
            opts.format_context,
        );

        if (candidates.len == 1) {
            const result = ResolveResult{ .resolved = .{
                .id = candidates[0].id,
                .name = candidates[0].name,
            } };
            // Free the candidates slice but keep the inner strings alive
            allocator.free(candidates);
            return result;
        }

        if (candidates.len > 1) {
            return .{ .ambiguous = candidates };
        }

        // 0 matches at this tier, try next
        allocator.free(candidates);
    }

    return .not_found;
}

const PatternFmt = enum { exact, prefix, contains };

fn queryByPattern(
    db: *CrmDb,
    allocator: std.mem.Allocator,
    table: [:0]const u8,
    name_col: [:0]const u8,
    query: []const u8,
    pattern_fmt: PatternFmt,
    match_type: MatchType,
    score: f32,
    company_id: ?[]const u8,
    format_context: ContextFormatFn,
) ![]MatchCandidate {
    // Build SQL based on pattern type and optional company_id filter
    var sql_buf: [512]u8 = undefined;
    const where_clause: []const u8 = switch (pattern_fmt) {
        .exact => "LOWER({s}) = LOWER(?1)",
        .prefix => "{s} LIKE ?1 || '%' COLLATE NOCASE",
        .contains => "{s} LIKE '%' || ?1 || '%' COLLATE NOCASE",
    };
    _ = where_clause;

    const sql_len = blk: {
        const company_filter = if (company_id != null) " AND company_id = ?2" else "";
        const result = switch (pattern_fmt) {
            .exact => std.fmt.bufPrint(&sql_buf, "SELECT id, {s} FROM {s} WHERE LOWER({s}) = LOWER(?1){s};", .{ name_col, table, name_col, company_filter }),
            .prefix => std.fmt.bufPrint(&sql_buf, "SELECT id, {s} FROM {s} WHERE {s} LIKE ?1 || '%' COLLATE NOCASE{s};", .{ name_col, table, name_col, company_filter }),
            .contains => std.fmt.bufPrint(&sql_buf, "SELECT id, {s} FROM {s} WHERE {s} LIKE '%' || ?1 || '%' COLLATE NOCASE{s};", .{ name_col, table, name_col, company_filter }),
        } catch return error.SqlQueryTooLong;
        break :blk result.len;
    };
    sql_buf[sql_len] = 0;
    const sql: [*:0]const u8 = sql_buf[0..sql_len :0];

    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, query.ptr, @intCast(query.len), SQLITE_STATIC);

    if (company_id) |cid| {
        _ = c.sqlite3_bind_text(stmt, 2, cid.ptr, @intCast(cid.len), SQLITE_STATIC);
    }

    var candidates = std.ArrayList(MatchCandidate).init(allocator);
    errdefer {
        for (candidates.items) |cand| {
            allocator.free(cand.id);
            allocator.free(cand.name);
            allocator.free(cand.context);
        }
        candidates.deinit();
    }

    while (true) {
        rc = c.sqlite3_step(stmt.?);
        if (rc != c.SQLITE_ROW) break;

        const raw_id = std.mem.span(c.sqlite3_column_text(stmt.?, 0));
        const raw_name = std.mem.span(c.sqlite3_column_text(stmt.?, 1));

        // Copy strings so they outlive the statement
        const id_copy = try allocator.dupe(u8, raw_id);
        errdefer allocator.free(id_copy);
        const name_copy = try allocator.dupe(u8, raw_name);
        errdefer allocator.free(name_copy);

        const ctx = format_context(db, allocator, raw_id) orelse
            try allocator.dupe(u8, "");

        try candidates.append(.{
            .id = id_copy,
            .name = name_copy,
            .match_type = match_type,
            .score = score,
            .context = ctx,
        });
    }

    return candidates.toOwnedSlice();
}

// ── Context formatting functions ───────────────────────────────────

fn formatCompanyContext(db: *CrmDb, allocator: std.mem.Allocator, id: []const u8) ?[]const u8 {
    const sql = "SELECT industry FROM companies WHERE id = ?1;";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    rc = c.sqlite3_step(stmt.?);
    if (rc != c.SQLITE_ROW) return null;

    const raw = c.sqlite3_column_text(stmt.?, 0);
    if (raw == null) return null;
    const industry = std.mem.span(raw);
    if (industry.len == 0) return null;

    return allocator.dupe(u8, industry) catch null;
}

fn formatContactContext(db: *CrmDb, allocator: std.mem.Allocator, id: []const u8) ?[]const u8 {
    const sql =
        \\SELECT c.role, COALESCE(co.name, '') FROM contacts c
        \\LEFT JOIN companies co ON c.company_id = co.id
        \\WHERE c.id = ?1;
    ;
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    rc = c.sqlite3_step(stmt.?);
    if (rc != c.SQLITE_ROW) return null;

    const raw_role = c.sqlite3_column_text(stmt.?, 0);
    const raw_company = c.sqlite3_column_text(stmt.?, 1);

    const role = if (raw_role != null) std.mem.span(raw_role) else "";
    const company = if (raw_company != null) std.mem.span(raw_company) else "";

    if (role.len > 0 and company.len > 0) {
        return std.fmt.allocPrint(allocator, "{s} at {s}", .{ role, company }) catch null;
    } else if (role.len > 0) {
        return allocator.dupe(u8, role) catch null;
    } else if (company.len > 0) {
        return std.fmt.allocPrint(allocator, "at {s}", .{company}) catch null;
    }
    return null;
}

fn formatDealContext(db: *CrmDb, allocator: std.mem.Allocator, id: []const u8) ?[]const u8 {
    const sql = "SELECT stage, value, currency FROM deals WHERE id = ?1;";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return null;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    rc = c.sqlite3_step(stmt.?);
    if (rc != c.SQLITE_ROW) return null;

    const raw_stage = c.sqlite3_column_text(stmt.?, 0);
    const stage = if (raw_stage != null) std.mem.span(raw_stage) else "";
    const value = c.sqlite3_column_double(stmt.?, 1);
    const raw_currency = c.sqlite3_column_text(stmt.?, 2);
    const currency = if (raw_currency != null) std.mem.span(raw_currency) else "USD";

    if (stage.len > 0) {
        return std.fmt.allocPrint(allocator, "{s}, {d:.0} {s}", .{ stage, value, currency }) catch null;
    }
    return null;
}

// ── Helper to free resolve results ─────────────────────────────────

/// Free all memory owned by a ResolveResult. Call this when the result
/// is no longer needed.
pub fn freeResult(allocator: std.mem.Allocator, result: *ResolveResult) void {
    switch (result.*) {
        .resolved => {},
        .ambiguous => |candidates| {
            for (candidates) |cand| {
                allocator.free(cand.id);
                allocator.free(cand.name);
                allocator.free(cand.context);
            }
            allocator.free(candidates);
        },
        .not_found => {},
    }
}

// ── Tests ──────────────────────────────────────────────────────────

fn insertTestCompany(db: *CrmDb, id: []const u8, name: []const u8, industry: ?[]const u8) !void {
    const sql = "INSERT INTO companies (id, name, industry, created_at, updated_at) VALUES (?1, ?2, ?3, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    _ = c.sqlite3_bind_text(stmt, 2, name.ptr, @intCast(name.len), SQLITE_STATIC);
    if (industry) |ind| {
        _ = c.sqlite3_bind_text(stmt, 3, ind.ptr, @intCast(ind.len), SQLITE_STATIC);
    } else {
        _ = c.sqlite3_bind_null(stmt, 3);
    }

    rc = c.sqlite3_step(stmt.?);
    if (rc != c.SQLITE_DONE) return error.SqliteStepFailed;
}

fn insertTestContact(db: *CrmDb, id: []const u8, name: []const u8, company_id: ?[]const u8, role: ?[]const u8) !void {
    const sql = "INSERT INTO contacts (id, name, company_id, role, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    _ = c.sqlite3_bind_text(stmt, 2, name.ptr, @intCast(name.len), SQLITE_STATIC);
    if (company_id) |cid| {
        _ = c.sqlite3_bind_text(stmt, 3, cid.ptr, @intCast(cid.len), SQLITE_STATIC);
    } else {
        _ = c.sqlite3_bind_null(stmt, 3);
    }
    if (role) |r| {
        _ = c.sqlite3_bind_text(stmt, 4, r.ptr, @intCast(r.len), SQLITE_STATIC);
    } else {
        _ = c.sqlite3_bind_null(stmt, 4);
    }

    rc = c.sqlite3_step(stmt.?);
    if (rc != c.SQLITE_DONE) return error.SqliteStepFailed;
}

fn insertTestDeal(db: *CrmDb, id: []const u8, title: []const u8, company_id: ?[]const u8, stage: []const u8, value: f64, currency: []const u8) !void {
    const sql = "INSERT INTO deals (id, title, company_id, stage, value, currency, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    if (rc != c.SQLITE_OK) return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(stmt);

    _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
    _ = c.sqlite3_bind_text(stmt, 2, title.ptr, @intCast(title.len), SQLITE_STATIC);
    if (company_id) |cid| {
        _ = c.sqlite3_bind_text(stmt, 3, cid.ptr, @intCast(cid.len), SQLITE_STATIC);
    } else {
        _ = c.sqlite3_bind_null(stmt, 3);
    }
    _ = c.sqlite3_bind_text(stmt, 4, stage.ptr, @intCast(stage.len), SQLITE_STATIC);
    _ = c.sqlite3_bind_double(stmt, 5, value);
    _ = c.sqlite3_bind_text(stmt, 6, currency.ptr, @intCast(currency.len), SQLITE_STATIC);

    rc = c.sqlite3_step(stmt.?);
    if (rc != c.SQLITE_DONE) return error.SqliteStepFailed;
}

test "resolve company - exact match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");

    var result = try resolveCompany(&db, allocator, "Northstar Technologies", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("c1", r.id);
            try std.testing.expectEqualStrings("Northstar Technologies", r.name);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve company - case insensitive match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");

    var result = try resolveCompany(&db, allocator, "northstar technologies", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("c1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve company - prefix match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");

    var result = try resolveCompany(&db, allocator, "Northstar", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("c1", r.id);
            try std.testing.expectEqualStrings("Northstar Technologies", r.name);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve company - contains match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Acme Northstar Inc", "Manufacturing");

    var result = try resolveCompany(&db, allocator, "Northstar", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("c1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve company - ambiguous results" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestCompany(&db, "c2", "Northstar Logistics", "Logistics");

    var result = try resolveCompany(&db, allocator, "Northstar", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .ambiguous => |candidates| {
            try std.testing.expectEqual(@as(usize, 2), candidates.len);
        },
        else => return error.ExpectedAmbiguous,
    }
}

test "resolve company - not found" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");

    var result = try resolveCompany(&db, allocator, "Acme Corp", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .not_found => {},
        else => return error.ExpectedNotFound,
    }
}

test "resolve company - by ID" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");

    var result = try resolveCompany(&db, allocator, null, "c1");
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("c1", r.id);
            try std.testing.expectEqualStrings("Northstar Technologies", r.name);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve company - by ID not found" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    var result = try resolveCompany(&db, allocator, null, "nonexistent");
    defer freeResult(allocator, &result);

    switch (result) {
        .not_found => {},
        else => return error.ExpectedNotFound,
    }
}

test "resolve contact - exact match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestContact(&db, "ct1", "James Chen", "c1", "VP Sales");

    var result = try resolveContact(&db, allocator, "James Chen", null, null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("ct1", r.id);
            try std.testing.expectEqualStrings("James Chen", r.name);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve contact - company scoped" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestCompany(&db, "c2", "Meridian Corp", "Finance");
    try insertTestContact(&db, "ct1", "James Chen", "c1", "VP Sales");
    try insertTestContact(&db, "ct2", "James Chen", "c2", "CTO");

    // Without company scope: ambiguous
    var result1 = try resolveContact(&db, allocator, "James Chen", null, null);
    defer freeResult(allocator, &result1);
    switch (result1) {
        .ambiguous => |candidates| {
            try std.testing.expectEqual(@as(usize, 2), candidates.len);
        },
        else => return error.ExpectedAmbiguous,
    }

    // With company scope: resolved to the one at Northstar
    var result2 = try resolveContact(&db, allocator, "James Chen", null, "c1");
    defer freeResult(allocator, &result2);
    switch (result2) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("ct1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve contact - by ID" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestContact(&db, "ct1", "James Chen", "c1", "VP Sales");

    var result = try resolveContact(&db, allocator, null, "ct1", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("ct1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve deal - exact match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestDeal(&db, "d1", "Northstar Enterprise License", "c1", "proposal", 50000.0, "USD");

    var result = try resolveDeal(&db, allocator, "Northstar Enterprise License", null, null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("d1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve deal - prefix match" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestDeal(&db, "d1", "Northstar Enterprise License", "c1", "proposal", 50000.0, "USD");

    var result = try resolveDeal(&db, allocator, "Northstar Enterprise", null, null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("d1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "resolve deal - by ID" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestDeal(&db, "d1", "Northstar Enterprise License", "c1", "proposal", 50000.0, "USD");

    var result = try resolveDeal(&db, allocator, null, "d1", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .resolved => |r| {
            try std.testing.expectEqualStrings("d1", r.id);
        },
        else => return error.ExpectedResolved,
    }
}

test "format candidates" {
    const allocator = std.testing.allocator;

    var candidates = [_]MatchCandidate{
        .{ .id = "ct1", .name = "James Chen", .match_type = .exact, .score = 1.0, .context = "VP Sales at Northstar" },
        .{ .id = "ct2", .name = "James Liu", .match_type = .exact, .score = 1.0, .context = "CTO at Meridian" },
    };

    const result = try formatCandidates(allocator, &candidates, "contact");
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Multiple contacts found:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1. James Chen (VP Sales at Northstar)") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "2. James Liu (CTO at Meridian)") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Which one did you mean?") != null);
}

test "resolve contact - context formatting" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestCompany(&db, "c2", "Meridian Corp", "Finance");
    try insertTestContact(&db, "ct1", "James Chen", "c1", "VP Sales");
    try insertTestContact(&db, "ct2", "James Chen", "c2", "CTO");

    var result = try resolveContact(&db, allocator, "James Chen", null, null);
    defer freeResult(allocator, &result);

    switch (result) {
        .ambiguous => |candidates| {
            // Verify context strings are populated
            for (candidates) |cand| {
                try std.testing.expect(cand.context.len > 0);
            }
        },
        else => return error.ExpectedAmbiguous,
    }
}

test "resolve company - context with industry" {
    const allocator = std.testing.allocator;
    var db = try CrmDb.init(allocator, ":memory:");
    defer db.deinit();

    try insertTestCompany(&db, "c1", "Northstar Technologies", "Software");
    try insertTestCompany(&db, "c2", "Northstar Logistics", "Logistics");

    var result = try resolveCompany(&db, allocator, "Northstar", null);
    defer freeResult(allocator, &result);

    switch (result) {
        .ambiguous => |candidates| {
            try std.testing.expectEqual(@as(usize, 2), candidates.len);
            // Both should have industry as context
            for (candidates) |cand| {
                try std.testing.expect(cand.context.len > 0);
            }
        },
        else => return error.ExpectedAmbiguous,
    }
}
