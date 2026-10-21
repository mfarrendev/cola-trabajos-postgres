/*
 * Medición (2026-10-20): en src/worker.ts, detener() dejó de esperar los
 * trabajos en curso (se borró `await Promise.allSettled([...this.enCurso.values()])`).
 * Cayó 1 de 6: «el apagado limpio termina lo que tiene y no toma más»; el
 * corredor reportó 1 failed | 5 passed (6). Revertido.
 */
import type { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Cola } from "../../src/cola.js";
import { Worker } from "../../src/worker.js";
import { baseEnMemoria, esperar, hasta } from "./ayuda.js";

let db: PGlite;
let cola: Cola;
beforeAll(async () => {
  db = await baseEnMemoria();
  cola = new Cola(db);
});
beforeEach(async () => {
  await db.exec("TRUNCATE trabajos, cron_tareas RESTART IDENTITY");
});
afterAll(async () => {
  await db.close();
});

const rapido = { sondeoMs: 20, cronMs: 0 };

describe("Worker", () => {
  it("procesa trabajos con su manejador y los marca completados", async () => {
    const vistos: number[] = [];
    for (let i = 0; i < 10; i++) await cola.encolar("sumar", { i });
    const w = new Worker(db, { ...rapido, concurrencia: 3, manejadores: { sumar: (p: { i: number }) => void vistos.push(p.i) } }).iniciar();
    await hasta(async () => (await cola.conteos()).completado === 10);
    await w.detener();
    expect(vistos.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it("no pasa de la concurrencia configurada", async () => {
    let activos = 0;
    let maximo = 0;
    for (let i = 0; i < 12; i++) await cola.encolar("lento", {});
    const w = new Worker(db, {
      ...rapido,
      concurrencia: 4,
      manejadores: {
        lento: async () => {
          maximo = Math.max(maximo, ++activos);
          await esperar(30);
          activos--;
        },
      },
    }).iniciar();
    await hasta(async () => (await cola.conteos()).completado === 12);
    await w.detener();
    expect(maximo).toBe(4);
  });

  it("un error reintenta con backoff y al agotar intentos queda muerto con el último error", async () => {
    let llamadas = 0;
    const { id } = await cola.encolar("falla", {}, { maxIntentos: 3 });
    const w = new Worker(db, {
      ...rapido,
      backoff: { baseMs: 10, maxMs: 50 },
      manejadores: {
        falla: () => {
          throw new Error(`fallo ${++llamadas}`);
        },
      },
    }).iniciar();
    await hasta(async () => (await cola.obtener(id))!.estado === "muerto");
    await w.detener();
    const t = (await cola.obtener(id))!;
    expect(llamadas).toBe(3);
    expect(t.intentos).toBe(3);
    expect(t.ultimoError).toMatch(/^Error: fallo 3/);
  });

  it("un tipo sin manejador falla con un error que lo dice", async () => {
    const { id } = await cola.encolar("desconocido", {}, { maxIntentos: 1 });
    const w = new Worker(db, { ...rapido, manejadores: {} }).iniciar();
    await hasta(async () => (await cola.obtener(id))!.estado === "muerto");
    await w.detener();
    expect((await cola.obtener(id))!.ultimoError).toMatch(/no hay manejador para el tipo «desconocido»/);
  });

  it("el apagado limpio termina lo que tiene y no toma más", async () => {
    let soltar!: () => void;
    const barrera = new Promise<void>((r) => (soltar = r));
    const a = await cola.encolar("bloquea", {});
    await cola.encolar("bloquea", {});
    const w = new Worker(db, { ...rapido, concurrencia: 1, manejadores: { bloquea: () => barrera } }).iniciar();
    await hasta(() => w.ocupados === 1);
    let detenido = false;
    const parando = w.detener().then(() => (detenido = true));
    await esperar(100);
    expect(detenido).toBe(false); // sigue esperando al que está en curso
    soltar();
    await parando;
    expect((await cola.obtener(a.id))!.estado).toBe("completado");
    expect(await cola.conteos()).toMatchObject({ completado: 1, pendiente: 1, ejecutando: 0 });
  });

  it("el heartbeat sostiene un trabajo más largo que el lease", async () => {
    const { id } = await cola.encolar("largo", {});
    let ejecuciones = 0;
    const w = new Worker(db, {
      ...rapido,
      leaseMs: 150,
      heartbeatMs: 40,
      manejadores: {
        largo: async () => {
          ejecuciones++;
          await esperar(500);
        },
      },
    }).iniciar();
    // Un segundo worker que intenta robar: con heartbeat no debería poder.
    const ladron = new Worker(db, { ...rapido, leaseMs: 150, heartbeatMs: 40, manejadores: { largo: () => void ejecuciones++ } }).iniciar();
    await hasta(async () => (await cola.obtener(id))!.estado === "completado");
    await Promise.all([w.detener(), ladron.detener()]);
    expect(ejecuciones).toBe(1);
    expect((await cola.obtener(id))!.intentos).toBe(1);
  });
});
