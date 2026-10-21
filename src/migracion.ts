import { readFile } from "node:fs/promises";
import type { Consultable } from "./tipos.js";

/** Ruta del SQL; vale igual desde src/ (tsx, vitest) que desde dist/. */
export const RUTA_MIGRACION = new URL("../sql/001_cola.sql", import.meta.url);

export async function sqlMigracion(): Promise<string> {
  return readFile(RUTA_MIGRACION, "utf8");
}

/**
 * Aplica la migración en una sola transacción.
 *
 * Se manda como UNA consulta de varias sentencias: el protocolo simple de
 * Postgres la corre entera en una transacción implícita, y eso funciona igual
 * con un `pg.Pool` (que si no podría mandar BEGIN y COMMIT por conexiones
 * distintas). El advisory lock del principio evita que dos procesos que
 * arrancan a la vez choquen: CREATE TABLE IF NOT EXISTS no es seguro en
 * paralelo (las dos sesiones pasan el IF NOT EXISTS y una revienta en
 * pg_type).
 */
export async function migrar(db: Consultable & { exec?: (sql: string) => Promise<unknown> }): Promise<void> {
  const sql =
    "SELECT pg_advisory_xact_lock(hashtext('cola-trabajos-postgres:migracion'));\n" + (await sqlMigracion());
  // PGlite solo acepta varias sentencias por exec(); pg las acepta por query()
  // sin parámetros.
  if (typeof db.exec === "function") await db.exec(sql);
  else await db.query(sql);
}
