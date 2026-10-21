/**
 * Benchmark simple: cuántos trabajos por segundo encola y procesa la cola
 * contra un Postgres real, con manejadores que no hacen nada (mide la cola,
 * no el trabajo).
 *
 *   N (5000)  WORKERS (4)  CONCURRENCIA (10)  COLA_PG_URL
 */
import pg from "pg";
import { Cola, Worker, migrar } from "../src/index.js";

const URL_PG = process.env.COLA_PG_URL ?? "postgres://postgres:local@127.0.0.1:55511/postgres";
const N = Number(process.env.N ?? 5000);
const WORKERS = Number(process.env.WORKERS ?? 4);
const CONCURRENCIA = Number(process.env.CONCURRENCIA ?? 10);

const admin = new pg.Client({ connectionString: URL_PG });
await admin.connect();
await admin.query("DROP SCHEMA IF EXISTS bench CASCADE");
await admin.query("CREATE SCHEMA bench");
const version = (await admin.query<{ server_version: string }>("SHOW server_version")).rows[0]!.server_version;
await admin.end();

const nuevoPool = (max: number) => new pg.Pool({ connectionString: URL_PG, max, options: "-c search_path=bench" });
const pool = nuevoPool(20);
await migrar(pool);
const cola = new Cola(pool);

// 1. Encolar de a uno, con 20 llamadas en paralelo.
let t = performance.now();
let siguiente = 0;
await Promise.all(
  Array.from({ length: 20 }, async () => {
    while (siguiente < N) {
      const i = siguiente++;
      await cola.encolar("nada", { i });
    }
  }),
);
const segEncolar = (performance.now() - t) / 1000;

// 2. Procesar: WORKERS workers, cada uno con su pool y su concurrencia.
const pools = Array.from({ length: WORKERS }, () => nuevoPool(CONCURRENCIA + 2));
t = performance.now();
const workers = pools.map((p) => new Worker(p, { concurrencia: CONCURRENCIA, sondeoMs: 10, cronMs: 0, manejadores: { nada: () => {} } }).iniciar());
for (;;) {
  const c = await cola.conteos();
  if (c.completado === N) break;
  await new Promise((r) => setTimeout(r, 25));
}
const segProcesar = (performance.now() - t) / 1000;
await Promise.all(workers.map((w) => w.detener()));
await Promise.all(pools.map((p) => p.end()));
await pool.query("DROP SCHEMA bench CASCADE");
await pool.end();

const f = (x: number) => x.toFixed(0);
console.log(`Postgres ${version}, Node ${process.version}, ${N} trabajos`);
console.log(`encolar (de a uno, 20 en paralelo): ${segEncolar.toFixed(2)} s, ${f(N / segEncolar)} trabajos/s`);
console.log(`procesar (${WORKERS} workers x concurrencia ${CONCURRENCIA}): ${segProcesar.toFixed(2)} s, ${f(N / segProcesar)} trabajos/s`);
