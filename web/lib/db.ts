import postgres, { Sql } from "postgres";

let sqlClient: Sql | null = null;

export function getSql(): Sql {
  if (sqlClient) {
    return sqlClient;
  }
  const dbUrl = process.env.NEON_DATABASE_URL;
  if (!dbUrl) {
    throw new Error("NEON_DATABASE_URL is not configured");
  }
  sqlClient = postgres(dbUrl, {
    ssl: "require",
    max: 1,
    idle_timeout: 20
  });
  return sqlClient;
}
