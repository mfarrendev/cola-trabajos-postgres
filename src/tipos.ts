/**
 * Lo mínimo que la cola necesita de una conexión: `pg.Pool`, `pg.Client` y
 * PGlite lo cumplen tal cual. Así las pruebas unitarias corren en memoria y
 * producción usa un pool de verdad, con el mismo SQL.
 */
export interface Consultable {
  query<F = Record<string, unknown>>(texto: string, parametros?: unknown[]): Promise<{ rows: F[] }>;
}

export type EstadoTrabajo = "pendiente" | "ejecutando" | "completado" | "muerto";

export interface Trabajo<P = unknown> {
  id: string;
  tipo: string;
  payload: P;
  estado: EstadoTrabajo;
  prioridad: number;
  runAt: Date;
  intentos: number;
  maxIntentos: number;
  claveIdempotencia: string | null;
  worker: string | null;
  leaseHasta: Date | null;
  ultimoError: string | null;
  creadoEn: Date;
  terminadoEn: Date | null;
}
