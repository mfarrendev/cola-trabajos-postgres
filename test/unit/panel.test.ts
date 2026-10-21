import type { PGlite } from "@electric-sql/pglite";
import type { Server } from "node:http";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Cola } from "../../src/cola.js";
import { iniciarPanel } from "../../src/panel.js";
import { baseEnMemoria } from "./ayuda.js";

let db: PGlite;
let cola: Cola;
let servidor: Server;
let url: string;
beforeAll(async () => {
  db = await baseEnMemoria();
  cola = new Cola(db);
  ({ servidor, url } = await iniciarPanel(db));
});
beforeEach(async () => {
  await db.exec("TRUNCATE trabajos, cron_tareas RESTART IDENTITY");
});
afterAll(async () => {
  servidor.close();
  await db.close();
});

async function matar(tipo: string, error: string): Promise<string> {
  const { id } = await cola.encolar(tipo, {}, { maxIntentos: 1 });
  await cola.tomar("w", 1, 30_000);
  await cola.fallar(id, "w", error, 0);
  return id;
}

describe("panel", () => {
  it("muestra conteos y muertos, con el error escapado", async () => {
    await matar("correo", "<script>alert(1)</script>");
    await cola.encolar("x", {});
    const r = await fetch(url + "/");
    const html = await r.text();
    expect(r.status).toBe(200);
    expect(html).toContain("<td>pendiente</td><td class=\"n\">1</td>");
    expect(html).toContain("<td>muerto</td><td class=\"n\">1</td>");
    expect(html).toContain("&lt;script&gt;alert(1)&lt;/script&gt;");
    expect(html).not.toContain("<script>alert");
  });

  it("la API devuelve lo mismo en JSON", async () => {
    await matar("correo", "boom");
    const j = (await (await fetch(url + "/api/estado")).json()) as { conteos: Record<string, number>; muertos: { ultimoError: string }[] };
    expect(j.conteos).toEqual({ pendiente: 0, ejecutando: 0, completado: 0, muerto: 1 });
    expect(j.muertos[0]!.ultimoError).toBe("boom");
  });

  it("reintentar un muerto lo devuelve a la cola", async () => {
    const id = await matar("correo", "boom");
    const r = await fetch(`${url}/reintentar/${id}`, { method: "POST", redirect: "manual" });
    expect(r.status).toBe(303);
    expect((await cola.obtener(id))!.estado).toBe("pendiente");
    const otra = await fetch(`${url}/reintentar/${id}`, { method: "POST", redirect: "manual" });
    expect(otra.status).toBe(409);
  });

  it("rechaza GET y otro origen en reintentar, y 404 en rutas desconocidas", async () => {
    const id = await matar("correo", "boom");
    expect((await fetch(`${url}/reintentar/${id}`)).status).toBe(405);
    const ajeno = await fetch(`${url}/reintentar/${id}`, { method: "POST", headers: { origin: "https://malo.example" } });
    expect(ajeno.status).toBe(403);
    expect((await cola.obtener(id))!.estado).toBe("muerto");
    expect((await fetch(`${url}/nada`)).status).toBe(404);
  });
});
