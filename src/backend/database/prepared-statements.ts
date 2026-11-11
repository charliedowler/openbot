import type { DatabaseSync, StatementSync } from "node:sqlite";

const statements = new WeakMap<DatabaseSync, Map<string, StatementSync>>();

export function preparedForGetOrAll(db: DatabaseSync, sql: string): StatementSync {
  let bySql = statements.get(db);
  if (!bySql) {
    bySql = new Map();
    statements.set(db, bySql);
  }
  let statement = bySql.get(sql);
  if (!statement) {
    statement = db.prepare(sql);
    bySql.set(sql, statement);
  }
  return statement;
}
