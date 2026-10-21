import { PGlite } from "@electric-sql/pglite";
import { migrar } from "../../src/migracion.js";

export async function baseEnMemoria(): Promise<PGlite> {
  const db = new PGlite();
  await migrar(db);
  return db;
}

export const esperar = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Espera a que `cond` sea verdadera, revisando cada 20 ms. */
export async function hasta(cond: () => Promise<boolean> | boolean, maxMs = 5000): Promise<void> {
  const fin = Date.now() + maxMs;
  while (Date.now() < fin) {
    if (await cond()) return;
    await esperar(20);
  }
  throw new Error(`la condición no se cumplió en ${maxMs} ms`);
}
