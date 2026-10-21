/**
 * Espera antes del reintento número `intento` (1 = primer reintento).
 *
 * Exponencial con tope y "jitter igual": la mitad de la espera es fija y la
 * otra mitad es azar. Con jitter completo (todo azar entre 0 y el tope) un
 * reintento puede salir casi inmediato aunque vaya en el décimo intento; con
 * la mitad fija la espera nunca baja de la mitad de lo que toca, y los
 * workers que fallaron juntos igual se dispersan.
 */
export interface OpcionesBackoff {
  /** Espera base en milisegundos para el primer reintento. */
  baseMs: number;
  /** Tope de la espera, en milisegundos. */
  maxMs: number;
}

export const BACKOFF_POR_OMISION: OpcionesBackoff = { baseMs: 1_000, maxMs: 10 * 60_000 };

export function esperaReintento(
  intento: number,
  opciones: OpcionesBackoff = BACKOFF_POR_OMISION,
  azar: () => number = Math.random,
): number {
  if (!Number.isInteger(intento) || intento < 1) {
    throw new RangeError(`intento debe ser un entero >= 1, vino ${intento}`);
  }
  // 2 ** 1023 ya es Infinity en la práctica; Math.min lo deja en el tope.
  const exponencial = Math.min(opciones.maxMs, opciones.baseMs * 2 ** (intento - 1));
  const mitad = exponencial / 2;
  return Math.round(mitad + azar() * mitad);
}
