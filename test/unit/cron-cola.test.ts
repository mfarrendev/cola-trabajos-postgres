import type { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Cola } from "../../src/cola.js";
import { baseEnMemoria } from "./ayuda.js";

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

const t0 = new Date("2026-10-20T12:00:30Z");
const min = (n: number) => new Date(t0.getTime() + n * 60_000);

describe("tickCron", () => {
  it("encola una vez por instante aunque se llame muchas veces", async () => {
    await cola.programar("resumen", "* * * * *", "resumen", { x: 1 }, { ahora: t0 });
    expect(await cola.tickCron(t0)).toBe(0); // la primera es 12:01
    expect(await cola.tickCron(min(1))).toBe(1);
    expect(await cola.tickCron(min(1))).toBe(0);
    expect(await cola.tickCron(min(2))).toBe(1);
    const r = await db.query<{ clave_idempotencia: string }>(
      "SELECT clave_idempotencia FROM trabajos ORDER BY id",
    );
    expect(r.rows.map((f) => f.clave_idempotencia)).toEqual([
      "cron:resumen:2026-10-20T12:01:00.000Z",
      "cron:resumen:2026-10-20T12:02:00.000Z",
    ]);
  });

  it("tras un apagón encola una sola ejecución atrasada y salta al futuro", async () => {
    await cola.programar("cada-5", "*/5 * * * *", "t", {}, { ahora: t0 });
    expect(await cola.tickCron(min(60))).toBe(1);
    const r = await db.query<{ proxima: Date }>("SELECT proxima FROM cron_tareas");
    expect(r.rows[0]!.proxima.toISOString()).toBe("2026-10-20T13:05:00.000Z");
  });

  it("dos lectores que vieron la misma tarea vencida no la duplican", async () => {
    await cola.programar("r", "* * * * *", "t", {}, { ahora: t0 });
    // Simula a dos workers que leyeron `proxima` antes de que el otro la
    // avanzara: los dos corren tickCron sobre el mismo estado.
    const [a, b] = await Promise.all([cola.tickCron(min(1)), cola.tickCron(min(1))]);
    expect(a + b).toBe(1);
    expect((await cola.conteos()).pendiente).toBe(1);
  });

  it("reprogramar con la misma expresión no mueve la próxima; con otra, sí", async () => {
    await cola.programar("r", "0 * * * *", "t", {}, { ahora: t0 });
    await cola.programar("r", "0 * * * *", "t", {}, { ahora: min(120) });
    let r = await db.query<{ proxima: Date }>("SELECT proxima FROM cron_tareas");
    expect(r.rows[0]!.proxima.toISOString()).toBe("2026-10-20T13:00:00.000Z");
    await cola.programar("r", "30 * * * *", "t", {}, { ahora: t0 });
    r = await db.query<{ proxima: Date }>("SELECT proxima FROM cron_tareas");
    expect(r.rows[0]!.proxima.toISOString()).toBe("2026-10-20T12:30:00.000Z");
  });

  it("rechaza una expresión inválida antes de tocar la base", async () => {
    await expect(cola.programar("mala", "61 * * * *", "t")).rejects.toThrow(/fuera de 0-59/);
    expect((await db.query("SELECT 1 FROM cron_tareas")).rows).toHaveLength(0);
  });
});
