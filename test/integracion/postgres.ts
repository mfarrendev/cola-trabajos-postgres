import pg from "pg";
import { migrar } from "../../src/migracion.js";

/**
 * Postgres real para las pruebas de integración. Por omisión el contenedor
 * del README (puerto 55511); COLA_PG_URL lo cambia. Si no hay servidor, las
 * pruebas se saltan con un aviso en vez de fallar.
 */
export const URL_PG = process.env.COLA_PG_URL ?? "postgres://postgres:local@127.0.0.1:55511/postgres";

export async function hayPostgres(): Promise<boolean> {
  const c = new pg.Client({ connectionString: URL_PG, connectionTimeoutMillis: 1500 });
  try {
    await c.connect();
    await c.query("SELECT 1");
    return true;
  } catch {
    return false;
  } finally {
    await c.end().catch(() => {});
  }
}

export const DISPONIBLE = await hayPostgres();
if (!DISPONIBLE) {
  console.warn(
    `\n[integración] AVISO: no hay Postgres en ${URL_PG}; se saltan las pruebas de integración.\n` +
      "  docker run -d --name pg-cola-demo -e POSTGRES_PASSWORD=local -p 127.0.0.1:55511:5432 postgres:17-alpine\n",
  );
}

/** Pool apuntado a un esquema propio (search_path), para que cada archivo de prueba no vea a los otros. */
export function poolEn(esquema: string, max = 10): pg.Pool {
  return new pg.Pool({ connectionString: URL_PG, max, options: `-c search_path=${esquema}` });
}

export async function esquemaNuevo(nombre: string): Promise<void> {
  const c = new pg.Client({ connectionString: URL_PG });
  await c.connect();
  await c.query(`DROP SCHEMA IF EXISTS ${nombre} CASCADE`);
  await c.query(`CREATE SCHEMA ${nombre}`);
  await c.end();
  const p = poolEn(nombre, 1);
  await migrar(p);
  await p.end();
}
