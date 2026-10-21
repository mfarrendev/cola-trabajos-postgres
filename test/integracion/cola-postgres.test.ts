/*
 * Integración contra Postgres 17 real.
 *
 * Medición (2026-10-20): en src/cola.ts se quitó `FOR UPDATE SKIP LOCKED` de
 * tomar(). Cayó 1 de 5: «4 workers en 4 procesos procesan 1000 trabajos sin
 * ejecutar ninguno dos veces» (1573 ejecuciones para 1000 trabajos). El corredor
 * reportó 1 failed | 4 passed (5). Revertido.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import type pg from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Cola } from "../../src/cola.js";
import { Worker } from "../../src/worker.js";
import { DISPONIBLE, esquemaNuevo, poolEn } from "./postgres.js";

const ESQUEMA = "prueba_cola";
const HIJO = fileURLToPath(new URL("./hijo-worker.ts", import.meta.url));
let pool: pg.Pool;
let cola: Cola;

const esperar = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function hasta(cond: () => Promise<boolean>, maxMs = 30_000): Promise<void> {
  const fin = Date.now() + maxMs;
  while (Date.now() < fin) {
    if (await cond()) return;
    await esperar(50);
  }
  throw new Error(`la condición no se cumplió en ${maxMs} ms`);
}

interface Hijo {
  proceso: ChildProcess;
  lineas: string[];
  esperarLinea: (re: RegExp) => Promise<string>;
  salida: Promise<{ codigo: number | null; senal: NodeJS.Signals | null }>;
}

function lanzarHijo(env: Record<string, string>): Hijo {
  const proceso = spawn(process.execPath, ["--import", "tsx", HIJO], {
    env: { ...process.env, ESQUEMA, ...env },
    stdio: ["ignore", "pipe", "inherit"],
  });
  const lineas: string[] = [];
  const oyentes: (() => void)[] = [];
  createInterface({ input: proceso.stdout! }).on("line", (l) => {
    lineas.push(l);
    oyentes.forEach((f) => f());
  });
  const salida = new Promise<{ codigo: number | null; senal: NodeJS.Signals | null }>((r) =>
    proceso.on("exit", (codigo, senal) => r({ codigo, senal })),
  );
  const esperarLinea = (re: RegExp) =>
    new Promise<string>((resolver, rechazar) => {
      const t = setTimeout(() => rechazar(new Error(`el hijo no escribió ${re}`)), 20_000);
      const mirar = () => {
        const l = lineas.find((x) => re.test(x));
        if (l) {
          clearTimeout(t);
          resolver(l);
        }
      };
      oyentes.push(mirar);
      mirar();
    });
  return { proceso, lineas, esperarLinea, salida };
}

describe.skipIf(!DISPONIBLE)("integración con Postgres 17", () => {
  beforeAll(async () => {
    await esquemaNuevo(ESQUEMA);
    pool = poolEn(ESQUEMA, 20);
    cola = new Cola(pool);
    await pool.query("CREATE TABLE ejecuciones (trabajo_id bigint NOT NULL, worker text NOT NULL)");
  });
  beforeEach(async () => {
    await pool.query("TRUNCATE trabajos, cron_tareas, ejecuciones RESTART IDENTITY");
  });
  afterAll(async () => {
    await pool?.end();
  });

  it("4 workers en 4 procesos procesan 1000 trabajos sin ejecutar ninguno dos veces", async () => {
    await pool.query(
      `INSERT INTO trabajos (tipo, payload) SELECT 'tarea', jsonb_build_object('i', g) FROM generate_series(1, 1000) g`,
    );
    const hijos = Array.from({ length: 4 }, () => lanzarHijo({ MODO: "registrar", CONCURRENCIA: "5" }));
    await Promise.all(hijos.map((h) => h.esperarLinea(/^LISTO$/)));
    await hasta(async () => (await cola.conteos()).completado === 1000, 60_000);
    hijos.forEach((h) => h.proceso.kill("SIGTERM"));
    const salidas = await Promise.all(hijos.map((h) => h.salida));

    const r = await pool.query<{ total: string; distintos: string; workers: string; repetidos: string }>(
      `SELECT count(*) AS total, count(DISTINCT trabajo_id) AS distintos, count(DISTINCT worker) AS workers,
              (SELECT count(*) FROM trabajos WHERE intentos <> 1) AS repetidos
         FROM ejecuciones`,
    );
    expect(r.rows[0]).toEqual({ total: "1000", distintos: "1000", workers: "4", repetidos: "0" });
    expect(salidas.every((s) => s.codigo === 0)).toBe(true);
  });

  it("un worker muerto con SIGKILL a mitad de un trabajo: el lease vence y otro lo reintenta", async () => {
    const { id } = await cola.encolar("tarea", {});
    const hijo = lanzarHijo({ MODO: "colgado", LEASE_MS: "1500" });
    const linea = await hijo.esperarLinea(/^TOMADO /);
    expect(linea).toBe(`TOMADO ${id}`);
    hijo.proceso.kill("SIGKILL");
    expect((await hijo.salida).senal).toBe("SIGKILL");

    const colgado = (await cola.obtener(id))!;
    expect(colgado.estado).toBe("ejecutando");
    expect(colgado.worker).toBe(`hijo-${hijo.proceso.pid}`);

    let ejecutado = 0;
    const w = new Worker(pool, { sondeoMs: 50, cronMs: 0, manejadores: { tarea: () => void ejecutado++ } }).iniciar();
    await hasta(async () => (await cola.obtener(id))!.estado === "completado", 10_000);
    await w.detener();
    const t = (await cola.obtener(id))!;
    expect(ejecutado).toBe(1);
    expect(t.intentos).toBe(2);
    expect(t.ultimoError).toBe(`lease vencido: el worker hijo-${hijo.proceso.pid} no terminó`);
  });

  it("SIGTERM: el hijo termina el trabajo que tiene, no toma el siguiente y sale con 0", async () => {
    const a = await cola.encolar("tarea", {}, { prioridad: 1 });
    const b = await cola.encolar("tarea", {});
    const hijo = lanzarHijo({ MODO: "lento" });
    await hijo.esperarLinea(new RegExp(`^TOMADO ${a.id}$`));
    hijo.proceso.kill("SIGTERM");
    expect(await hijo.salida).toEqual({ codigo: 0, senal: null });
    expect((await cola.obtener(a.id))!.estado).toBe("completado");
    expect((await cola.obtener(b.id))!.estado).toBe("pendiente");
  });

  it("idempotencia: 50 encolados simultáneos con la misma clave dejan un solo trabajo", async () => {
    const rs = await Promise.all(
      Array.from({ length: 50 }, (_, i) => cola.encolar("tarea", { i }, { claveIdempotencia: "pedido-77" })),
    );
    expect(rs.filter((r) => r.creado)).toHaveLength(1);
    expect(new Set(rs.map((r) => r.id)).size).toBe(1);
    expect((await pool.query("SELECT count(*)::int AS n FROM trabajos")).rows[0]).toEqual({ n: 1 });
  });

  it("cron con 3 workers: cada instante se encola y se ejecuta una sola vez", async () => {
    const pools = [poolEn(ESQUEMA, 4), poolEn(ESQUEMA, 4), poolEn(ESQUEMA, 4)];
    try {
      const colas = pools.map((p) => new Cola(p));
      const t0 = new Date("2026-10-20T12:00:30Z");
      await colas[0]!.programar("cada-minuto", "* * * * *", "tarea", {}, { ahora: t0 });
      // Diez minutos simulados; en cada uno los tres «workers» llaman al tick
      // cinco veces a la vez, compitiendo por la misma tarea vencida.
      for (let m = 1; m <= 10; m++) {
        const ahora = new Date(t0.getTime() + m * 60_000);
        await Promise.all(colas.flatMap((c) => Array.from({ length: 5 }, () => c.tickCron(ahora))));
      }
      const r = await pool.query<{ n: number; distintos: number }>(
        "SELECT count(*)::int AS n, count(DISTINCT clave_idempotencia)::int AS distintos FROM trabajos",
      );
      expect(r.rows[0]).toEqual({ n: 10, distintos: 10 });

      // Y con el reloj real: tres Workers con su propio tick de cron
      // encuentran la tarea vencida y la ejecutan una sola vez entre todos.
      await pool.query("TRUNCATE trabajos, ejecuciones");
      await pool.query("UPDATE cron_tareas SET proxima = date_trunc('second', now()) - interval '1 second'");
      let ejecuciones = 0;
      const workers = pools.map((p) =>
        new Worker(p, { cronMs: 20, sondeoMs: 20, manejadores: { tarea: () => void ejecuciones++ } }).iniciar(),
      );
      await esperar(1000);
      await Promise.all(workers.map((w) => w.detener()));
      expect(ejecuciones).toBe(1);
      expect((await cola.conteos()).completado).toBe(1);
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
  });
});
