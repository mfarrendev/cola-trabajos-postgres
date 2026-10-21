import { parsearCron, siguienteEjecucion } from "./cron.js";
import type { Consultable, EstadoTrabajo, Trabajo } from "./tipos.js";

export interface OpcionesEncolar {
  /** Mayor número, antes se toma. Por omisión 0. */
  prioridad?: number;
  /** No se toma antes de este instante. */
  runAt?: Date;
  /** Atajo de runAt: tantos milisegundos desde ahora (reloj de la base). */
  retrasoMs?: number;
  /** Intentos totales, contando el primero. Por omisión 5. */
  maxIntentos?: number;
  /**
   * Si ya existe un trabajo con esta clave (en cualquier estado), no se crea
   * otro y se devuelve el existente con `creado: false`.
   */
  claveIdempotencia?: string;
}

export interface ResultadoEncolar {
  id: string;
  creado: boolean;
}

export interface ConteoPorEstado {
  pendiente: number;
  ejecutando: number;
  completado: number;
  muerto: number;
}

interface FilaTrabajo {
  id: string;
  tipo: string;
  payload: unknown;
  estado: EstadoTrabajo;
  prioridad: number;
  run_at: Date | string;
  intentos: number;
  max_intentos: number;
  clave_idempotencia: string | null;
  worker: string | null;
  lease_hasta: Date | string | null;
  ultimo_error: string | null;
  creado_en: Date | string;
  terminado_en: Date | string | null;
}

const fecha = (v: Date | string) => (v instanceof Date ? v : new Date(v));
const fechaONull = (v: Date | string | null) => (v === null ? null : fecha(v));

function aTrabajo<P>(f: FilaTrabajo): Trabajo<P> {
  return {
    id: String(f.id),
    tipo: f.tipo,
    payload: f.payload as P,
    estado: f.estado,
    prioridad: f.prioridad,
    runAt: fecha(f.run_at),
    intentos: f.intentos,
    maxIntentos: f.max_intentos,
    claveIdempotencia: f.clave_idempotencia,
    worker: f.worker,
    leaseHasta: fechaONull(f.lease_hasta),
    ultimoError: f.ultimo_error,
    creadoEn: fecha(f.creado_en),
    terminadoEn: fechaONull(f.terminado_en),
  };
}

/** Intervalo en milisegundos como expresión SQL sobre un parámetro. */
const ms = (n: number) => `make_interval(secs => $${n}::double precision / 1000)`;

/**
 * Operaciones de la cola sobre una conexión. Cada método es UNA sentencia
 * SQL (salvo el tick de cron, que son dos y no necesita transacción): así
 * funciona sobre un pool sin pedir una conexión dedicada y no hay estado a
 * medias si el proceso muere entre dos llamadas.
 *
 * Todo el tiempo se mide con el reloj de Postgres (`now()`), no con el de
 * cada worker: con relojes de máquinas distintas un lease podría vencer
 * antes para uno que para otro.
 */
export class Cola {
  constructor(private readonly db: Consultable) {}

  async encolar<P>(tipo: string, payload: P, op: OpcionesEncolar = {}): Promise<ResultadoEncolar> {
    if (op.runAt !== undefined && op.retrasoMs !== undefined) {
      throw new TypeError("runAt y retrasoMs no van juntos");
    }
    const filas = await this.db.query<{ id: string }>(
      `INSERT INTO trabajos (tipo, payload, prioridad, run_at, max_intentos, clave_idempotencia)
       VALUES ($1, $2::jsonb, $3,
               COALESCE($4::timestamptz, now() + ${ms(5)}),
               $6, $7)
       ON CONFLICT (clave_idempotencia) WHERE clave_idempotencia IS NOT NULL DO NOTHING
       RETURNING id`,
      [
        tipo,
        JSON.stringify(payload ?? null),
        op.prioridad ?? 0,
        op.runAt?.toISOString() ?? null,
        op.retrasoMs ?? 0,
        op.maxIntentos ?? 5,
        op.claveIdempotencia ?? null,
      ],
    );
    const nueva = filas.rows[0];
    if (nueva) return { id: String(nueva.id), creado: true };
    // La clave ya existía. Se lee en una sentencia aparte: con ON CONFLICT DO
    // NOTHING, RETURNING no devuelve la fila que chocó.
    const existente = await this.db.query<{ id: string }>(
      `SELECT id FROM trabajos WHERE clave_idempotencia = $1`,
      [op.claveIdempotencia],
    );
    const fila = existente.rows[0];
    if (!fila) throw new Error("conflicto de idempotencia sin fila: no debería pasar");
    return { id: String(fila.id), creado: false };
  }

  /**
   * Toma hasta `cuantos` trabajos listos y los deja a nombre de `worker` con
   * un lease de `leaseMs`. Listos son los pendientes con run_at vencido y los
   * que estaban ejecutándose con un lease vencido (su worker murió o quedó
   * colgado sin heartbeat).
   *
   * FOR UPDATE SKIP LOCKED hace que dos workers que toman a la vez se salten
   * las filas que el otro ya bloqueó en vez de esperar o tomar la misma. La
   * fila bloqueada se vuelve a evaluar contra el WHERE con su versión más
   * nueva (READ COMMITTED), así que una que otro worker ya tomó y confirmó
   * tampoco pasa.
   */
  async tomar(worker: string, cuantos: number, leaseMs: number): Promise<Trabajo[]> {
    if (cuantos < 1) return [];
    await this.enterrarLeasesAgotados();
    const r = await this.db.query<FilaTrabajo>(
      `WITH elegidos AS (
         SELECT id FROM trabajos
          WHERE (estado = 'pendiente' AND run_at <= now())
             OR (estado = 'ejecutando' AND lease_hasta < now() AND intentos < max_intentos)
          ORDER BY prioridad DESC, run_at, id
          LIMIT $2
          FOR UPDATE SKIP LOCKED
       )
       UPDATE trabajos t
          SET estado = 'ejecutando',
              worker = $1,
              intentos = t.intentos + 1,
              lease_hasta = now() + ${ms(3)},
              ultimo_error = CASE WHEN t.estado = 'ejecutando'
                                  THEN 'lease vencido: el worker ' || coalesce(t.worker, '?') || ' no terminó'
                                  ELSE t.ultimo_error END,
              actualizado_en = now()
         FROM elegidos
        WHERE t.id = elegidos.id
       RETURNING t.*`,
      [worker, cuantos, leaseMs],
    );
    return r.rows.map((f) => aTrabajo(f)).sort((a, b) => b.prioridad - a.prioridad || Number(a.id) - Number(b.id));
  }

  /**
   * Un trabajo cuyo worker murió en el último intento no puede volver a la
   * cola: pasa a muerto. Si no, un trabajo que tumba al worker (memoria,
   * segfault de una dependencia nativa) se reintentaría para siempre.
   */
  private async enterrarLeasesAgotados(): Promise<void> {
    await this.db.query(
      `UPDATE trabajos
          SET estado = 'muerto',
              ultimo_error = 'lease vencido en el intento ' || intentos || ' de ' || max_intentos
                             || ': el worker ' || coalesce(worker, '?') || ' no terminó',
              worker = NULL, lease_hasta = NULL,
              terminado_en = now(), actualizado_en = now()
        WHERE estado = 'ejecutando' AND lease_hasta < now() AND intentos >= max_intentos`,
    );
  }

  /**
   * Extiende el lease. Devuelve false si el trabajo ya no es de este worker
   * (el lease venció y otro lo tomó): el que llama debe dejar de trabajar en él.
   */
  async heartbeat(id: string, worker: string, leaseMs: number): Promise<boolean> {
    const r = await this.db.query(
      `UPDATE trabajos SET lease_hasta = now() + ${ms(3)}, actualizado_en = now()
        WHERE id = $1 AND worker = $2 AND estado = 'ejecutando'
        RETURNING id`,
      [id, worker, leaseMs],
    );
    return r.rows.length === 1;
  }

  /** Marca completado. False si el trabajo ya no era de este worker. */
  async completar(id: string, worker: string): Promise<boolean> {
    const r = await this.db.query(
      `UPDATE trabajos
          SET estado = 'completado', worker = NULL, lease_hasta = NULL,
              terminado_en = now(), actualizado_en = now()
        WHERE id = $1 AND worker = $2 AND estado = 'ejecutando'
        RETURNING id`,
      [id, worker],
    );
    return r.rows.length === 1;
  }

  /**
   * Registra un fallo. Si quedan intentos vuelve a pendiente con run_at
   * desplazado `esperaMs`; si no, pasa a muerto con el error. Devuelve el
   * estado resultante, o null si el trabajo ya no era de este worker.
   */
  async fallar(id: string, worker: string, error: string, esperaMs: number): Promise<"pendiente" | "muerto" | null> {
    const r = await this.db.query<{ estado: "pendiente" | "muerto" }>(
      `UPDATE trabajos
          SET estado = CASE WHEN intentos >= max_intentos THEN 'muerto' ELSE 'pendiente' END,
              run_at = CASE WHEN intentos >= max_intentos THEN run_at ELSE now() + ${ms(4)} END,
              terminado_en = CASE WHEN intentos >= max_intentos THEN now() ELSE NULL END,
              ultimo_error = $3,
              worker = NULL, lease_hasta = NULL, actualizado_en = now()
        WHERE id = $1 AND worker = $2 AND estado = 'ejecutando'
        RETURNING estado`,
      [id, worker, error.slice(0, 8000), esperaMs],
    );
    return r.rows[0]?.estado ?? null;
  }

  /** Devuelve un muerto a la cola con los intentos en cero. False si no estaba muerto. */
  async reintentarMuerto(id: string): Promise<boolean> {
    const r = await this.db.query(
      `UPDATE trabajos
          SET estado = 'pendiente', intentos = 0, run_at = now(),
              terminado_en = NULL, actualizado_en = now()
        WHERE id = $1 AND estado = 'muerto'
        RETURNING id`,
      [id],
    );
    return r.rows.length === 1;
  }

  async obtener<P = unknown>(id: string): Promise<Trabajo<P> | null> {
    const r = await this.db.query<FilaTrabajo>(`SELECT * FROM trabajos WHERE id = $1`, [id]);
    const f = r.rows[0];
    return f ? aTrabajo<P>(f) : null;
  }

  async conteos(): Promise<ConteoPorEstado> {
    const r = await this.db.query<{ estado: EstadoTrabajo; n: string | number }>(
      `SELECT estado, count(*) AS n FROM trabajos GROUP BY estado`,
    );
    const c: ConteoPorEstado = { pendiente: 0, ejecutando: 0, completado: 0, muerto: 0 };
    for (const f of r.rows) c[f.estado] = Number(f.n);
    return c;
  }

  async ultimosMuertos(limite = 20): Promise<Trabajo[]> {
    const r = await this.db.query<FilaTrabajo>(
      `SELECT * FROM trabajos WHERE estado = 'muerto' ORDER BY terminado_en DESC, id DESC LIMIT $1`,
      [limite],
    );
    return r.rows.map((f) => aTrabajo(f));
  }

  /**
   * Registra (o actualiza) una tarea recurrente. Si la expresión cambia, la
   * próxima ejecución se recalcula desde ahora.
   */
  async programar(
    nombre: string,
    expresion: string,
    tipo: string,
    payload: unknown = {},
    op: { maxIntentos?: number; ahora?: Date } = {},
  ): Promise<void> {
    const c = parsearCron(expresion);
    const proxima = siguienteEjecucion(c, op.ahora ?? new Date());
    await this.db.query(
      `INSERT INTO cron_tareas (nombre, expresion, tipo, payload, max_intentos, proxima)
       VALUES ($1, $2, $3, $4::jsonb, $5, $6)
       ON CONFLICT (nombre) DO UPDATE
         SET tipo = EXCLUDED.tipo, payload = EXCLUDED.payload, max_intentos = EXCLUDED.max_intentos,
             proxima = CASE WHEN cron_tareas.expresion = EXCLUDED.expresion
                            THEN cron_tareas.proxima ELSE EXCLUDED.proxima END,
             expresion = EXCLUDED.expresion`,
      [nombre, expresion, tipo, JSON.stringify(payload), op.maxIntentos ?? 5, proxima.toISOString()],
    );
  }

  /**
   * Encola las tareas recurrentes que vencieron. Lo pueden llamar todos los
   * workers a la vez: el trabajo de cada ejecución lleva la clave de
   * idempotencia `cron:<nombre>:<instante ISO>`, así que aunque tres workers
   * vean la misma tarea vencida, el índice único deja entrar uno solo. Después
   * se avanza `proxima` con un UPDATE condicionado al valor que se leyó
   * (compare-and-set): si otro ya la avanzó, este no hace nada.
   *
   * Si el sistema estuvo apagado y se perdieron varias ejecuciones, se encola
   * UNA (la más antigua pendiente) y se salta a la siguiente posterior a
   * `ahora`. Ponerse al día con todas suele ser lo que no se quiere.
   *
   * Devuelve cuántos trabajos nuevos encoló ESTA llamada.
   */
  async tickCron(ahora: Date = new Date()): Promise<number> {
    const vencidas = await this.db.query<{
      nombre: string;
      expresion: string;
      tipo: string;
      payload: unknown;
      max_intentos: number;
      proxima: Date | string;
    }>(`SELECT * FROM cron_tareas WHERE proxima <= $1`, [ahora.toISOString()]);
    let encolados = 0;
    for (const t of vencidas.rows) {
      const instante = fecha(t.proxima);
      const r = await this.encolar(t.tipo, t.payload, {
        claveIdempotencia: `cron:${t.nombre}:${instante.toISOString()}`,
        maxIntentos: t.max_intentos,
      });
      if (r.creado) encolados++;
      const siguiente = siguienteEjecucion(t.expresion, ahora > instante ? ahora : instante);
      await this.db.query(
        `UPDATE cron_tareas SET proxima = $3, ultima = $2 WHERE nombre = $1 AND proxima = $2`,
        [t.nombre, instante.toISOString(), siguiente.toISOString()],
      );
    }
    return encolados;
  }
}
