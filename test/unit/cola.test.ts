/*
 * Medición (2026-10-20), dos mutaciones en src/cola.ts, cada una revertida:
 *
 * 1. Se quitó la condición de dueño y estado en completar() (`AND worker = $2
 *    AND estado = 'ejecutando'`). Cayó
 *    1 de 14 pruebas: «un worker que perdió el lease no puede completar ni
 *    fallar». El corredor reportó 1 failed | 13 passed (14).
 * 2. Se quitó `intentos < max_intentos` del lease vencido en tomar() y la
 *    llamada a enterrarLeasesAgotados(). Cayó 1 de 14: «un lease vencido en
 *    el último intento pasa a muerto, no vuelve a la cola».
 */
import type { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Cola } from "../../src/cola.js";
import { baseEnMemoria, esperar } from "./ayuda.js";

let db: PGlite;
let cola: Cola;

// Una base por archivo (crear PGlite cuesta ~1 s); cada prueba parte vacía.
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

describe("encolar y tomar", () => {
  it("toma por prioridad y después por orden de llegada", async () => {
    const a = await cola.encolar("x", { n: 1 });
    const b = await cola.encolar("x", { n: 2 }, { prioridad: 10 });
    const c = await cola.encolar("x", { n: 3 });
    const tomados = await cola.tomar("w1", 10, 30_000);
    expect(tomados.map((t) => t.id)).toEqual([b.id, a.id, c.id]);
    expect(tomados[0]!.payload).toEqual({ n: 2 });
    expect(tomados.every((t) => t.estado === "ejecutando" && t.worker === "w1" && t.intentos === 1)).toBe(true);
  });

  it("no toma lo que ya está tomado", async () => {
    await cola.encolar("x", {});
    expect(await cola.tomar("w1", 5, 30_000)).toHaveLength(1);
    expect(await cola.tomar("w2", 5, 30_000)).toHaveLength(0);
  });

  it("respeta run_at: un trabajo diferido no sale antes de tiempo", async () => {
    await cola.encolar("x", {}, { retrasoMs: 300 });
    await cola.encolar("x", {}, { runAt: new Date(Date.now() + 3_600_000) });
    expect(await cola.tomar("w1", 5, 30_000)).toHaveLength(0);
    await esperar(350);
    expect(await cola.tomar("w1", 5, 30_000)).toHaveLength(1);
  });

  it("respeta el límite de cuántos tomar", async () => {
    for (let i = 0; i < 5; i++) await cola.encolar("x", { i });
    expect(await cola.tomar("w1", 2, 30_000)).toHaveLength(2);
    expect(await cola.tomar("w1", 0, 30_000)).toHaveLength(0);
  });
});

describe("idempotencia", () => {
  it("la misma clave devuelve el trabajo existente y no crea otro", async () => {
    const p = await cola.encolar("correo", { a: 1 }, { claveIdempotencia: "factura-881" });
    const q = await cola.encolar("correo", { a: 2 }, { claveIdempotencia: "factura-881" });
    expect(p.creado).toBe(true);
    expect(q).toEqual({ id: p.id, creado: false });
    expect((await cola.conteos()).pendiente).toBe(1);
    expect((await cola.obtener(p.id))!.payload).toEqual({ a: 1 });
  });

  it("también después de que el primero terminó", async () => {
    const p = await cola.encolar("correo", {}, { claveIdempotencia: "k" });
    await cola.tomar("w", 1, 30_000);
    await cola.completar(p.id, "w");
    expect((await cola.encolar("correo", {}, { claveIdempotencia: "k" })).creado).toBe(false);
  });

  it("sin clave no hay deduplicación", async () => {
    await cola.encolar("x", {});
    await cola.encolar("x", {});
    expect((await cola.conteos()).pendiente).toBe(2);
  });
});

describe("fallos, reintentos y muertos", () => {
  it("un fallo con intentos restantes vuelve a pendiente, diferido", async () => {
    const { id } = await cola.encolar("x", {}, { maxIntentos: 3 });
    await cola.tomar("w", 1, 30_000);
    expect(await cola.fallar(id, "w", "boom", 200)).toBe("pendiente");
    const t = (await cola.obtener(id))!;
    expect(t.ultimoError).toBe("boom");
    expect(t.worker).toBeNull();
    expect(await cola.tomar("w", 1, 30_000)).toHaveLength(0);
    await esperar(250);
    expect((await cola.tomar("w", 1, 30_000))[0]!.intentos).toBe(2);
  });

  it("el último intento fallido lo deja muerto con su error, y se puede reintentar", async () => {
    const { id } = await cola.encolar("x", {}, { maxIntentos: 2 });
    await cola.tomar("w", 1, 30_000);
    await cola.fallar(id, "w", "primero", 0);
    await cola.tomar("w", 1, 30_000);
    expect(await cola.fallar(id, "w", "segundo", 0)).toBe("muerto");
    const muertos = await cola.ultimosMuertos();
    expect(muertos.map((m) => [m.id, m.ultimoError])).toEqual([[id, "segundo"]]);
    expect(await cola.tomar("w", 1, 30_000)).toHaveLength(0);

    expect(await cola.reintentarMuerto(id)).toBe(true);
    expect(await cola.reintentarMuerto(id)).toBe(false);
    const t = (await cola.tomar("w", 1, 30_000))[0]!;
    expect([t.id, t.intentos]).toEqual([id, 1]);
  });

  it("un worker que perdió el lease no puede completar ni fallar", async () => {
    const { id } = await cola.encolar("x", {});
    await cola.tomar("lento", 1, 50);
    await esperar(80);
    expect((await cola.tomar("rapido", 1, 30_000))[0]!.id).toBe(id);
    expect(await cola.completar(id, "lento")).toBe(false);
    expect(await cola.fallar(id, "lento", "tarde", 0)).toBeNull();
    expect(await cola.heartbeat(id, "lento", 30_000)).toBe(false);
    expect((await cola.obtener(id))!.worker).toBe("rapido");
    expect(await cola.completar(id, "rapido")).toBe(true);
  });

  it("un lease vencido vuelve a la cola con un error que lo explica", async () => {
    const { id } = await cola.encolar("x", {});
    await cola.tomar("caido", 1, 50);
    await esperar(80);
    const t = (await cola.tomar("otro", 1, 30_000))[0]!;
    expect(t.id).toBe(id);
    expect(t.intentos).toBe(2);
    expect(t.ultimoError).toMatch(/lease vencido.*caido/);
  });

  it("el heartbeat mantiene el lease y otro worker no lo roba", async () => {
    const { id } = await cola.encolar("x", {});
    await cola.tomar("w", 1, 100);
    for (let i = 0; i < 4; i++) {
      await esperar(50);
      expect(await cola.heartbeat(id, "w", 100)).toBe(true);
    }
    expect(await cola.tomar("ladron", 1, 30_000)).toHaveLength(0);
  });

  it("un lease vencido en el último intento pasa a muerto, no vuelve a la cola", async () => {
    const { id } = await cola.encolar("x", {}, { maxIntentos: 1 });
    await cola.tomar("caido", 1, 50);
    await esperar(80);
    expect(await cola.tomar("otro", 1, 30_000)).toHaveLength(0);
    const t = (await cola.obtener(id))!;
    expect(t.estado).toBe("muerto");
    expect(t.ultimoError).toMatch(/intento 1 de 1/);
  });
});

describe("conteos", () => {
  it("cuenta por estado, con ceros para los vacíos", async () => {
    await cola.encolar("x", {});
    const { id } = await cola.encolar("x", {});
    await cola.encolar("x", {}, { prioridad: 5 });
    const [t] = await cola.tomar("w", 1, 30_000);
    await cola.completar(t!.id, "w");
    await cola.tomar("w", 1, 30_000);
    expect(await cola.conteos()).toEqual({ pendiente: 1, ejecutando: 1, completado: 1, muerto: 0 });
    expect(id).toBeTruthy();
  });
});
