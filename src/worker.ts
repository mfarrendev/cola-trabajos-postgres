import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { BACKOFF_POR_OMISION, esperaReintento, type OpcionesBackoff } from "./backoff.js";
import { Cola } from "./cola.js";
import type { Consultable, Trabajo } from "./tipos.js";

export interface ContextoTrabajo {
  trabajo: Trabajo;
  /**
   * Se aborta cuando el worker pierde el lease (el heartbeat no pudo
   * renovarlo y otro worker pudo haberlo tomado). Un manejador largo debería
   * revisarlo y cortar: lo que haga después ya no cuenta.
   */
  signal: AbortSignal;
}

export type Manejador<P = any> = (payload: P, ctx: ContextoTrabajo) => Promise<void> | void;

export interface OpcionesWorker {
  /** Manejadores por tipo de trabajo. */
  manejadores: Record<string, Manejador>;
  /** Trabajos simultáneos en este proceso. Por omisión 1. */
  concurrencia?: number;
  /** Duración del lease. Por omisión 30 s. */
  leaseMs?: number;
  /** Cada cuánto se renueva el lease. Por omisión un tercio del lease. */
  heartbeatMs?: number;
  /** Espera cuando no hay nada que tomar. Por omisión 500 ms. */
  sondeoMs?: number;
  /** Cada cuánto revisar tareas cron vencidas; 0 lo apaga. Por omisión 5 s. */
  cronMs?: number;
  backoff?: OpcionesBackoff;
  /** Nombre en la columna `worker`. Por omisión host:pid:aleatorio. */
  id?: string;
  /** Para errores que no son de un trabajo (la base se cayó, etc.). */
  alError?: (e: unknown) => void;
}

const dormir = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolver) => {
    if (signal.aborted) return resolver();
    const t = setTimeout(() => {
      signal.removeEventListener("abort", alto);
      resolver();
    }, ms);
    const alto = () => {
      clearTimeout(t);
      resolver();
    };
    signal.addEventListener("abort", alto, { once: true });
  });

function describirError(e: unknown): string {
  if (e instanceof Error) return e.stack ?? `${e.name}: ${e.message}`;
  try {
    return typeof e === "string" ? e : JSON.stringify(e);
  } catch {
    return String(e);
  }
}

/**
 * Un worker: toma trabajos, los corre con su manejador, mantiene el lease
 * vivo con heartbeats y registra el resultado.
 *
 * `detener()` es el apagado limpio: deja de tomar trabajos nuevos y espera a
 * que terminen los que ya tiene. No los aborta; si el proceso igual muere,
 * los leases vencen y otro worker los retoma.
 */
export class Worker {
  readonly id: string;
  private readonly cola: Cola;
  private readonly op: Required<Omit<OpcionesWorker, "id" | "alError">> & { alError: ((e: unknown) => void) | undefined };
  private readonly enCurso = new Map<string, Promise<void>>();
  private readonly parar = new AbortController();
  private bucle: Promise<void> | null = null;
  private ultimoCron = 0;
  /** Despierta al bucle cuando se libera un hueco de concurrencia. */
  private avisarHueco: (() => void) | null = null;

  constructor(db: Consultable, opciones: OpcionesWorker) {
    this.cola = new Cola(db);
    const leaseMs = opciones.leaseMs ?? 30_000;
    this.op = {
      manejadores: opciones.manejadores,
      concurrencia: opciones.concurrencia ?? 1,
      leaseMs,
      heartbeatMs: opciones.heartbeatMs ?? Math.max(50, Math.floor(leaseMs / 3)),
      sondeoMs: opciones.sondeoMs ?? 500,
      cronMs: opciones.cronMs ?? 5_000,
      backoff: opciones.backoff ?? BACKOFF_POR_OMISION,
      alError: opciones.alError,
    };
    if (this.op.heartbeatMs >= leaseMs) {
      throw new RangeError("heartbeatMs debe ser menor que leaseMs, o el lease vence entre latidos");
    }
    this.id = opciones.id ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
  }

  /** Arranca el bucle. Devuelve de inmediato. */
  iniciar(): this {
    if (this.bucle) throw new Error("el worker ya está corriendo");
    this.bucle = this.correr();
    return this;
  }

  get ocupados(): number {
    return this.enCurso.size;
  }

  /** Deja de tomar trabajos y espera a que terminen los que tiene. */
  async detener(): Promise<void> {
    this.parar.abort();
    this.avisarHueco?.();
    await this.bucle;
    await Promise.allSettled([...this.enCurso.values()]);
  }

  private async correr(): Promise<void> {
    const alto = this.parar.signal;
    while (!alto.aborted) {
      try {
        await this.quizasCron();
        const libres = this.op.concurrencia - this.enCurso.size;
        if (libres <= 0) {
          await new Promise<void>((r) => (this.avisarHueco = r));
          this.avisarHueco = null;
          continue;
        }
        const trabajos = await this.cola.tomar(this.id, libres, this.op.leaseMs);
        for (const t of trabajos) {
          const p = this.ejecutar(t).finally(() => {
            this.enCurso.delete(t.id);
            this.avisarHueco?.();
          });
          this.enCurso.set(t.id, p);
        }
        // Si se llenó lo que se pidió, probablemente hay más: no dormir.
        if (trabajos.length < libres) await dormir(this.op.sondeoMs, alto);
      } catch (e) {
        this.op.alError?.(e);
        await dormir(this.op.sondeoMs, alto);
      }
    }
  }

  private async quizasCron(): Promise<void> {
    if (this.op.cronMs <= 0) return;
    const ahora = Date.now();
    if (ahora - this.ultimoCron < this.op.cronMs) return;
    this.ultimoCron = ahora;
    await this.cola.tickCron();
  }

  private async ejecutar(t: Trabajo): Promise<void> {
    const perdido = new AbortController();
    const latido = setInterval(() => {
      this.cola.heartbeat(t.id, this.id, this.op.leaseMs).then(
        (sigue) => {
          if (!sigue) perdido.abort(new Error("lease perdido"));
        },
        (e) => this.op.alError?.(e),
      );
    }, this.op.heartbeatMs);
    try {
      const manejador = this.op.manejadores[t.tipo];
      if (!manejador) throw new Error(`no hay manejador para el tipo «${t.tipo}»`);
      await manejador(t.payload, { trabajo: t, signal: perdido.signal });
      clearInterval(latido);
      await this.cola.completar(t.id, this.id);
    } catch (e) {
      clearInterval(latido);
      try {
        await this.cola.fallar(t.id, this.id, describirError(e), esperaReintento(t.intentos, this.op.backoff));
      } catch (e2) {
        // Si ni siquiera se pudo registrar el fallo, el lease vencerá y el
        // trabajo se reintentará igual.
        this.op.alError?.(e2);
      }
    } finally {
      clearInterval(latido);
    }
  }
}

/**
 * Conecta SIGTERM y SIGINT al apagado limpio de uno o más workers. Una
 * segunda señal mientras se espera sale de inmediato (código 130): es lo que
 * espera quien aprieta Ctrl+C dos veces.
 */
export function apagarConSenales(workers: Worker[], alTerminar: () => Promise<void> | void = () => {}): void {
  let apagando = false;
  const manejar = (senal: NodeJS.Signals) => {
    if (apagando) process.exit(130);
    apagando = true;
    process.stderr.write(`${senal}: no se toman trabajos nuevos; esperando los que están en curso\n`);
    void Promise.all(workers.map((w) => w.detener()))
      .then(alTerminar)
      .then(
        () => process.exit(0),
        (e) => {
          process.stderr.write(`error al apagar: ${describirError(e)}\n`);
          process.exit(1);
        },
      );
  };
  process.on("SIGTERM", manejar);
  process.on("SIGINT", manejar);
}
