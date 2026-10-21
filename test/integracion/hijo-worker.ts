/**
 * Worker en un proceso aparte, para las pruebas que necesitan matarlo de
 * verdad (SIGKILL) o apagarlo con SIGTERM.
 *
 *   ESQUEMA   esquema de la base
 *   MODO      registrar | colgado | lento
 *   LEASE_MS  duración del lease
 *
 * Escribe «TOMADO <id>» en stdout cuando empieza un trabajo y «LISTO» cuando
 * está corriendo.
 */
import { Worker, apagarConSenales } from "../../src/worker.js";
import { poolEn } from "./postgres.js";

const esquema = process.env.ESQUEMA!;
const modo = process.env.MODO ?? "registrar";
const pool = poolEn(esquema, 8);

const manejador = async (_p: unknown, { trabajo }: { trabajo: { id: string } }) => {
  process.stdout.write(`TOMADO ${trabajo.id}\n`);
  if (modo === "colgado") await new Promise(() => {}); // nunca termina
  if (modo === "lento") await new Promise((r) => setTimeout(r, 600));
  await pool.query("INSERT INTO ejecuciones (trabajo_id, worker) VALUES ($1, $2)", [trabajo.id, w.id]);
};

const w = new Worker(pool, {
  id: `hijo-${process.pid}`,
  concurrencia: Number(process.env.CONCURRENCIA ?? 1),
  leaseMs: Number(process.env.LEASE_MS ?? 30_000),
  sondeoMs: 20,
  cronMs: 0,
  manejadores: { tarea: manejador },
  alError: (e) => process.stderr.write(`hijo: ${String(e)}\n`),
}).iniciar();
apagarConSenales([w], () => pool.end());
process.stdout.write("LISTO\n");
