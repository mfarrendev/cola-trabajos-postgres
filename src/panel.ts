import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Cola } from "./cola.js";
import type { Consultable, Trabajo } from "./tipos.js";

/**
 * Panel mínimo: conteos por estado, los últimos muertos con su error y un
 * botón para reintentar cada uno. node:http, sin framework.
 *
 * No tiene autenticación. Se escucha en 127.0.0.1 por omisión; exponerlo
 * fuera de la máquina pide ponerlo detrás de algo que autentique.
 *
 * Rutas:
 *   GET  /                    HTML
 *   GET  /api/estado          { conteos, muertos } en JSON
 *   POST /reintentar/:id      devuelve el muerto a la cola (303 a /)
 */
export interface OpcionesPanel {
  puerto?: number;
  host?: string;
  /** Cuántos muertos listar. Por omisión 20. */
  limiteMuertos?: number;
}

const escapar = (s: string) =>
  s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);

function html(conteos: Record<string, number>, muertos: Trabajo[]): string {
  const filasConteo = Object.entries(conteos)
    .map(([estado, n]) => `<tr><td>${estado}</td><td class="n">${n}</td></tr>`)
    .join("");
  const filasMuertos = muertos.length
    ? muertos
        .map(
          (t) => `<tr>
  <td class="n">${escapar(t.id)}</td><td>${escapar(t.tipo)}</td><td class="n">${t.intentos}/${t.maxIntentos}</td>
  <td>${t.terminadoEn ? escapar(t.terminadoEn.toISOString()) : ""}</td>
  <td><pre>${escapar((t.ultimoError ?? "").split("\n").slice(0, 4).join("\n"))}</pre></td>
  <td><form method="post" action="/reintentar/${encodeURIComponent(t.id)}"><button>Reintentar</button></form></td>
</tr>`,
        )
        .join("")
    : `<tr><td colspan="6">No hay trabajos muertos.</td></tr>`;
  return `<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cola de trabajos</title>
<style>
body{font:14px/1.4 system-ui,sans-serif;margin:24px;color:#222;background:#fafafa}
table{border-collapse:collapse;margin-bottom:24px;background:#fff}
td,th{border:1px solid #ddd;padding:6px 10px;text-align:left;vertical-align:top}
.n{text-align:right;font-variant-numeric:tabular-nums}
pre{margin:0;white-space:pre-wrap;max-width:60ch;font-size:12px}
</style></head><body>
<h1>Cola de trabajos</h1>
<table><tr><th>Estado</th><th>Trabajos</th></tr>${filasConteo}</table>
<h2>Últimos muertos</h2>
<table><tr><th>Id</th><th>Tipo</th><th>Intentos</th><th>Murió</th><th>Último error</th><th></th></tr>${filasMuertos}</table>
</body></html>`;
}

/**
 * Rechaza un POST que viene de otro origen. El navegador manda Origin en los
 * POST de formularios; sin esto, cualquier página abierta en la misma máquina
 * podría reintentar muertos con un formulario oculto.
 */
function mismoOrigen(req: IncomingMessage): boolean {
  const origen = req.headers.origin;
  if (origen === undefined) return true; // curl y clientes que no son navegador
  try {
    return new URL(origen).host === req.headers.host;
  } catch {
    return false;
  }
}

export function crearManejadorPanel(db: Consultable, op: OpcionesPanel = {}) {
  const cola = new Cola(db);
  const limite = op.limiteMuertos ?? 20;
  return async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    try {
      const url = new URL(req.url ?? "/", "http://panel");
      if (req.method === "GET" && url.pathname === "/") {
        const [conteos, muertos] = await Promise.all([cola.conteos(), cola.ultimosMuertos(limite)]);
        res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
        res.end(html({ ...conteos }, muertos));
        return;
      }
      if (req.method === "GET" && url.pathname === "/api/estado") {
        const [conteos, muertos] = await Promise.all([cola.conteos(), cola.ultimosMuertos(limite)]);
        res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
        res.end(JSON.stringify({ conteos, muertos }));
        return;
      }
      const m = /^\/reintentar\/(\d+)$/.exec(url.pathname);
      if (m) {
        if (req.method !== "POST") {
          res.writeHead(405, { allow: "POST" }).end();
          return;
        }
        if (!mismoOrigen(req)) {
          res.writeHead(403, { "content-type": "text/plain; charset=utf-8" }).end("origen distinto");
          return;
        }
        const ok = await cola.reintentarMuerto(m[1]!);
        if (!ok) {
          res.writeHead(409, { "content-type": "text/plain; charset=utf-8" }).end("el trabajo no está muerto");
          return;
        }
        res.writeHead(303, { location: "/" }).end();
        return;
      }
      res.writeHead(404, { "content-type": "text/plain; charset=utf-8" }).end("no existe");
    } catch (e) {
      res.writeHead(500, { "content-type": "text/plain; charset=utf-8" }).end("error interno");
      process.stderr.write(`panel: ${e instanceof Error ? e.stack : String(e)}\n`);
    }
  };
}

export async function iniciarPanel(db: Consultable, op: OpcionesPanel = {}): Promise<{ servidor: Server; url: string }> {
  const manejador = crearManejadorPanel(db, op);
  const servidor = createServer((req, res) => void manejador(req, res));
  await new Promise<void>((r) => servidor.listen(op.puerto ?? 0, op.host ?? "127.0.0.1", r));
  const { address, port } = servidor.address() as AddressInfo;
  return { servidor, url: `http://${address}:${port}` };
}
