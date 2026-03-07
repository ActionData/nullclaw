//! CRM SQLite schema module — structured data layer for companies,
//! contacts, deals, activities, and stage history.
//!
//! This module initializes and manages the CRM database schema,
//! separate from nullclaw's LanceDB/SQLite memory store used for RAG.

const std = @import("std");
const log = std.log.scoped(.crm_schema);

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const SQLITE_STATIC: c.sqlite3_destructor_type = null;
const BUSY_TIMEOUT_MS: c_int = 5000;
const DEFAULT_DB_PATH: [:0]const u8 = "/data/crm.db";

/// CRM database handle. Opens (or creates) a SQLite database and
/// ensures the full CRM schema is applied.
pub const CrmDb = struct {
    db: ?*c.sqlite3,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Opens (or creates) the SQLite database at `path` and runs
    /// schema creation.  Pass `:memory:` for an in-memory database
    /// (useful for tests).
    pub fn init(allocator: std.mem.Allocator, path: [*:0]const u8) !Self {
        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }
        if (db) |d| {
            _ = c.sqlite3_busy_timeout(d, BUSY_TIMEOUT_MS);
        }

        var self = Self{ .db = db, .allocator = allocator };
        try self.configurePragmas();
        try self.ensureSchema();
        return self;
    }

    /// Closes the database connection.
    pub fn deinit(self: *Self) void {
        if (self.db) |db| {
            _ = c.sqlite3_close(db);
            self.db = null;
        }
    }

    /// Configures recommended pragmas for the CRM database.
    fn configurePragmas(self: *Self) !void {
        const pragmas = [_][:0]const u8{
            "PRAGMA journal_mode = WAL;",
            "PRAGMA synchronous  = NORMAL;",
            "PRAGMA temp_store   = MEMORY;",
            "PRAGMA cache_size   = -2000;",
            "PRAGMA foreign_keys = ON;",
        };
        for (pragmas) |pragma| {
            var err_msg: [*c]u8 = null;
            const rc = c.sqlite3_exec(self.db, pragma, null, null, &err_msg);
            if (rc != c.SQLITE_OK) {
                self.logExecFailure("pragma", pragma, rc, err_msg);
            }
            if (err_msg) |msg| c.sqlite3_free(msg);
        }
    }

    /// Creates all CRM tables and indexes if they don't already exist.
    /// Safe to call multiple times (idempotent).
    pub fn ensureSchema(self: *Self) !void {
        try self.execSql(schema_ddl);
        try self.execSql(index_ddl);
        try self.execSql(version_ddl);
    }

    /// Executes a SQL string that may contain multiple statements.
    pub fn execSql(self: *Self, sql: [*:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            self.logExecFailure("exec", sql, rc, err_msg);
            if (err_msg) |msg| c.sqlite3_free(msg);
            return error.SqliteExecFailed;
        }
        if (err_msg) |msg| c.sqlite3_free(msg);
    }

    /// Generates a UUID v4-style hex string (32 hex chars, no dashes).
    pub fn generateUuid(self: *Self) [32]u8 {
        _ = self;
        var buf: [16]u8 = undefined;
        std.crypto.random.bytes(&buf);
        // Set version (4) and variant (RFC 4122)
        buf[6] = (buf[6] & 0x0f) | 0x40;
        buf[8] = (buf[8] & 0x3f) | 0x80;
        return std.fmt.bytesToHex(buf, .lower);
    }

    fn logExecFailure(self: *Self, context: []const u8, sql: [*:0]const u8, rc: c_int, err_msg: [*c]u8) void {
        _ = sql;
        if (err_msg) |msg| {
            const msg_text = std.mem.span(msg);
            log.warn("sqlite {s} failed (rc={d}): {s}", .{ context, rc, msg_text });
            return;
        }
        if (self.db) |db| {
            const msg_text = std.mem.span(c.sqlite3_errmsg(db));
            log.warn("sqlite {s} failed (rc={d}): {s}", .{ context, rc, msg_text });
            return;
        }
        log.warn("sqlite {s} failed (rc={d})", .{ context, rc });
    }

    // ── Schema DDL ─────────────────────────────────────────────────

    const schema_ddl: [:0]const u8 =
        \\CREATE TABLE IF NOT EXISTS companies (
        \\  id          TEXT PRIMARY KEY,
        \\  name        TEXT NOT NULL,
        \\  industry    TEXT,
        \\  size        TEXT,
        \\  website     TEXT,
        \\  notes       TEXT,
        \\  created_at  TEXT NOT NULL,
        \\  updated_at  TEXT NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS contacts (
        \\  id          TEXT PRIMARY KEY,
        \\  name        TEXT NOT NULL,
        \\  company_id  TEXT REFERENCES companies(id),
        \\  role        TEXT,
        \\  email       TEXT,
        \\  phone       TEXT,
        \\  notes       TEXT,
        \\  created_at  TEXT NOT NULL,
        \\  updated_at  TEXT NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS deals (
        \\  id          TEXT PRIMARY KEY,
        \\  title       TEXT NOT NULL,
        \\  company_id  TEXT REFERENCES companies(id),
        \\  contact_id  TEXT REFERENCES contacts(id),
        \\  stage       TEXT NOT NULL,
        \\  value       REAL,
        \\  currency    TEXT DEFAULT 'USD',
        \\  close_date  TEXT,
        \\  next_step   TEXT,
        \\  notes       TEXT,
        \\  created_at  TEXT NOT NULL,
        \\  updated_at  TEXT NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS activities (
        \\  id              TEXT PRIMARY KEY,
        \\  type            TEXT NOT NULL,
        \\  contact_id      TEXT REFERENCES contacts(id),
        \\  deal_id         TEXT REFERENCES deals(id),
        \\  company_id      TEXT REFERENCES companies(id),
        \\  rep_id          TEXT,
        \\  summary         TEXT NOT NULL,
        \\  date            TEXT NOT NULL,
        \\  follow_up_date  TEXT,
        \\  follow_up_note  TEXT,
        \\  created_at      TEXT NOT NULL
        \\);
        \\
        \\CREATE TABLE IF NOT EXISTS stage_history (
        \\  id          TEXT PRIMARY KEY,
        \\  deal_id     TEXT REFERENCES deals(id),
        \\  from_stage  TEXT,
        \\  to_stage    TEXT NOT NULL,
        \\  notes       TEXT,
        \\  changed_at  TEXT NOT NULL
        \\);
    ;

    const index_ddl: [:0]const u8 =
        \\CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts(company_id);
        \\CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name COLLATE NOCASE);
        \\CREATE INDEX IF NOT EXISTS idx_deals_company ON deals(company_id);
        \\CREATE INDEX IF NOT EXISTS idx_deals_contact ON deals(contact_id);
        \\CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);
        \\CREATE INDEX IF NOT EXISTS idx_activities_contact ON activities(contact_id);
        \\CREATE INDEX IF NOT EXISTS idx_activities_deal ON activities(deal_id);
        \\CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);
        \\CREATE INDEX IF NOT EXISTS idx_activities_followup ON activities(follow_up_date);
        \\CREATE INDEX IF NOT EXISTS idx_stage_history_deal ON stage_history(deal_id);
        \\CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name COLLATE NOCASE);
        \\CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);
        \\CREATE INDEX IF NOT EXISTS idx_deals_value ON deals(value);
        \\CREATE INDEX IF NOT EXISTS idx_deals_title ON deals(title COLLATE NOCASE);
        \\CREATE INDEX IF NOT EXISTS idx_activities_company_id ON activities(company_id);
        \\CREATE INDEX IF NOT EXISTS idx_activities_rep_id ON activities(rep_id);
        \\CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(type);
        \\CREATE INDEX IF NOT EXISTS idx_stage_history_changed_at ON stage_history(changed_at);
    ;

    const version_ddl: [:0]const u8 =
        \\CREATE TABLE IF NOT EXISTS schema_version (
        \\    version INTEGER NOT NULL,
        \\    applied_at TEXT NOT NULL DEFAULT (datetime('now')),
        \\    description TEXT
        \\);
        \\INSERT INTO schema_version (version, description) SELECT 1, 'Initial CRM schema' WHERE NOT EXISTS (SELECT 1 FROM schema_version);
    ;
};

// ── Tests ──────────────────────────────────────────────────────────

test "crm schema init with in-memory db" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();
}

test "crm schema creates all tables" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    const expected_tables = [_][]const u8{
        "companies",
        "contacts",
        "deals",
        "activities",
        "stage_history",
        "schema_version",
    };

    for (expected_tables) |table_name| {
        const sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
        try std.testing.expectEqual(@as(c_int, c.SQLITE_OK), rc);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, table_name.ptr, @intCast(table_name.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        try std.testing.expectEqual(@as(c_int, c.SQLITE_ROW), rc);

        const count = c.sqlite3_column_int(stmt.?, 0);
        try std.testing.expectEqual(@as(c_int, 1), count);
    }
}

test "crm schema creates all indexes" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    const expected_indexes = [_][]const u8{
        "idx_contacts_company",
        "idx_contacts_name",
        "idx_deals_company",
        "idx_deals_contact",
        "idx_deals_stage",
        "idx_activities_contact",
        "idx_activities_deal",
        "idx_activities_date",
        "idx_activities_followup",
        "idx_stage_history_deal",
        "idx_companies_name",
        "idx_contacts_email",
        "idx_deals_value",
        "idx_deals_title",
        "idx_activities_company_id",
        "idx_activities_rep_id",
        "idx_activities_type",
        "idx_stage_history_changed_at",
    };

    for (expected_indexes) |index_name| {
        const sql = "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?1;";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
        try std.testing.expectEqual(@as(c_int, c.SQLITE_OK), rc);
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, index_name.ptr, @intCast(index_name.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt.?);
        try std.testing.expectEqual(@as(c_int, c.SQLITE_ROW), rc);

        const count = c.sqlite3_column_int(stmt.?, 0);
        try std.testing.expectEqual(@as(c_int, 1), count);
    }
}

test "crm schema ensureSchema is idempotent" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    // ensureSchema already ran in init; call it again
    try db.ensureSchema();

    // Verify tables still exist
    const sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('companies','contacts','deals','activities','stage_history','schema_version');";
    var stmt: ?*c.sqlite3_stmt = null;
    var rc = c.sqlite3_prepare_v2(db.db, sql, -1, &stmt, null);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_OK), rc);
    defer _ = c.sqlite3_finalize(stmt);

    rc = c.sqlite3_step(stmt.?);
    try std.testing.expectEqual(@as(c_int, c.SQLITE_ROW), rc);

    const count = c.sqlite3_column_int(stmt.?, 0);
    try std.testing.expectEqual(@as(c_int, 6), count);
}

test "crm schema foreign keys enforced" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    // Try inserting a contact with a non-existent company_id — should fail
    const sql =
        \\INSERT INTO contacts (id, name, company_id, created_at, updated_at)
        \\VALUES ('c1', 'Test Contact', 'nonexistent', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
    ;
    var err_msg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db.db, sql, null, null, &err_msg);
    if (err_msg) |msg| c.sqlite3_free(msg);
    try std.testing.expect(rc != c.SQLITE_OK);
}

test "crm schema uuid generation" {
    var db = try CrmDb.init(std.testing.allocator, ":memory:");
    defer db.deinit();

    const uuid1 = db.generateUuid();
    const uuid2 = db.generateUuid();

    // UUIDs should be 32 hex characters
    try std.testing.expectEqual(@as(usize, 32), uuid1.len);
    try std.testing.expectEqual(@as(usize, 32), uuid2.len);

    // UUIDs should be different
    try std.testing.expect(!std.mem.eql(u8, &uuid1, &uuid2));

    // All characters should be valid hex
    for (uuid1) |ch| {
        try std.testing.expect((ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f'));
    }
}
