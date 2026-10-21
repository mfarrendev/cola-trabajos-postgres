/**
 * Ejemplo: una cola de correos simulados con un worker, una tarea cron y el
 * panel. No envía nada de verdad: el «envío» es una espera y un log.
 *
 *   docker run -d --name pg-cola-demo -e POSTGRES_PASSWORD=local -p 127.0.0.1:55511:5432 postgres:17-alpine
 *   npm run ejemplo
 *
 * Variables: COLA_PG_URL, PUERTO (panel, 8080), DURACION_MS (si se da, el
 * proceso se manda SIGTERM a sí mismo después de ese tiempo).
 */
import pg from "pg";
import { Cola, Worker, apagarConSenales, iniciarPanel, migrar } from "../src/index.js";

const URL_PG = process.env.COLA_PG_URL ?? "postgres://postgres:local@127.0.0.1:55511/postgres";

const admin = new pg.Client({ connectionString: URL_PG });
await admin.connect();
await admin.query("CREATE SCHEMA IF NOT EXISTS ejemplo");
await admin.end();

const pool = new pg.Pool({ connectionString: URL_PG, max: 8, options: "-c search_path=ejemplo" });
await migrar(pool);
const cola = new Cola(pool);

interface Correo {
  para: string;
  asunto: string;
}

for (const para of ["ana@example.com", "luis@example.com", "rebota@example.com"]) {
  const r = await cola.encolar<Correo>("enviar-correo", { para, asunto: "Bienvenida" }, {
    claveIdempotencia: `bienvenida:${para}`,
    maxIntentos: 3,
  });
  console.log(`encolar ${para}: id ${r.id}${r.creado ? "" : " (ya existía, no se duplica)"}`);
}
await cola.encolar<Correo>("enviar-correo", { para: "urgente@example.com", asunto: "Alerta" }, { prioridad: 10 });
await cola.programar("resumen-diario", "0 8 * * *", "resumen", {});

const worker = new Worker(pool, {
  concurrencia: 2,
  leaseMs: 10_000,
  backoff: { baseMs: 200, maxMs: 2_000 },
  manejadores: {
    "enviar-correo": async (c: Correo, { trabajo }) => {
      await new Promise((r) => setTimeout(r, 100));
      if (c.para.startsWith("rebota")) throw new Error(`el servidor de ${c.para} rechazó el mensaje`);
      console.log(`enviado a ${c.para} («${c.asunto}», intento ${trabajo.intentos})`);
    },
    resumen: async () => console.log("resumen diario generado"),
  },
  alError: (e) => console.error("worker:", e),
}).iniciar();

const { servidor, url } = await iniciarPanel(pool, { puerto: Number(process.env.PUERTO ?? 8080) });
console.log(`panel en ${url}`);

apagarConSenales([worker], async () => {
  servidor.close();
  console.log("estado final:", await cola.conteos());
  await pool.end();
});

if (process.env.DURACION_MS) setTimeout(() => process.kill(process.pid, "SIGTERM"), Number(process.env.DURACION_MS));
