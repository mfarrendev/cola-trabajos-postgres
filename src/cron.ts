/**
 * Parser de expresiones cron de cinco campos, sin dependencias.
 *
 *   minuto  hora  día-del-mes  mes  día-de-la-semana
 *   0-59    0-23  1-31         1-12 0-7 (0 y 7 son domingo)
 *
 * Cada campo acepta `*`, un número, un rango `a-b`, una lista `a,b,c` y un
 * paso `*\/n`, `a-b/n` o `a/n`. No acepta nombres (`JAN`, `MON`) ni los
 * atajos `@daily`: se rechazan con un error en vez de interpretarse a medias.
 *
 * Todo se calcula en UTC. Una cola que corre en varias máquinas no puede
 * depender de la zona horaria de cada una.
 *
 * Día del mes y día de la semana siguen la regla de cron clásico: si los dos
 * están restringidos (ninguno es `*`), basta con que calce UNO.
 */

export interface ExpresionCron {
  readonly fuente: string;
  readonly minutos: ReadonlySet<number>;
  readonly horas: ReadonlySet<number>;
  readonly diasMes: ReadonlySet<number>;
  readonly meses: ReadonlySet<number>;
  readonly diasSemana: ReadonlySet<number>;
  readonly diaMesLibre: boolean;
  readonly diaSemanaLibre: boolean;
}

export class ErrorCron extends Error {
  override name = "ErrorCron";
}

interface Limites {
  nombre: string;
  min: number;
  max: number;
}

const CAMPOS: readonly Limites[] = [
  { nombre: "minuto", min: 0, max: 59 },
  { nombre: "hora", min: 0, max: 23 },
  { nombre: "día del mes", min: 1, max: 31 },
  { nombre: "mes", min: 1, max: 12 },
  { nombre: "día de la semana", min: 0, max: 7 },
];

function entero(texto: string, campo: Limites): number {
  if (!/^\d+$/.test(texto)) {
    throw new ErrorCron(`${campo.nombre}: «${texto}» no es un número`);
  }
  const n = Number(texto);
  if (n < campo.min || n > campo.max) {
    throw new ErrorCron(`${campo.nombre}: ${n} está fuera de ${campo.min}-${campo.max}`);
  }
  return n;
}

function parsearCampo(texto: string, campo: Limites): Set<number> {
  const valores = new Set<number>();
  for (const parte of texto.split(",")) {
    if (parte === "") throw new ErrorCron(`${campo.nombre}: elemento vacío en «${texto}»`);
    const [rango, pasoTexto, sobra] = parte.split("/");
    if (sobra !== undefined || rango === undefined) {
      throw new ErrorCron(`${campo.nombre}: «${parte}» tiene más de un «/»`);
    }
    let paso = 1;
    if (pasoTexto !== undefined) {
      if (!/^\d+$/.test(pasoTexto) || Number(pasoTexto) === 0) {
        throw new ErrorCron(`${campo.nombre}: paso inválido «${pasoTexto}»`);
      }
      paso = Number(pasoTexto);
    }
    let desde: number;
    let hasta: number;
    if (rango === "*") {
      desde = campo.min;
      hasta = campo.max;
    } else if (rango.includes("-")) {
      const [a, b, otra] = rango.split("-");
      if (a === undefined || b === undefined || otra !== undefined) {
        throw new ErrorCron(`${campo.nombre}: rango inválido «${rango}»`);
      }
      desde = entero(a, campo);
      hasta = entero(b, campo);
      if (desde > hasta) {
        throw new ErrorCron(`${campo.nombre}: rango al revés «${rango}»`);
      }
    } else {
      desde = entero(rango, campo);
      // «5/15» significa desde 5 hasta el final, de 15 en 15.
      hasta = pasoTexto !== undefined ? campo.max : desde;
    }
    for (let v = desde; v <= hasta; v += paso) valores.add(v);
  }
  return valores;
}

export function parsearCron(expresion: string): ExpresionCron {
  const partes = expresion.trim().split(/\s+/);
  if (partes.length !== 5) {
    throw new ErrorCron(`se esperan 5 campos y vinieron ${partes.length}: «${expresion}»`);
  }
  const [m, h, dm, mes, ds] = partes as [string, string, string, string, string];
  const diasSemana = parsearCampo(ds, CAMPOS[4]!);
  if (diasSemana.has(7)) {
    diasSemana.delete(7);
    diasSemana.add(0);
  }
  return {
    fuente: expresion,
    minutos: parsearCampo(m, CAMPOS[0]!),
    horas: parsearCampo(h, CAMPOS[1]!),
    diasMes: parsearCampo(dm, CAMPOS[2]!),
    meses: parsearCampo(mes, CAMPOS[3]!),
    diasSemana,
    diaMesLibre: dm.startsWith("*"),
    diaSemanaLibre: ds.startsWith("*"),
  };
}

function calzaDia(c: ExpresionCron, fecha: Date): boolean {
  const dm = c.diasMes.has(fecha.getUTCDate());
  const ds = c.diasSemana.has(fecha.getUTCDay());
  if (c.diaMesLibre && c.diaSemanaLibre) return true;
  if (c.diaMesLibre) return ds;
  if (c.diaSemanaLibre) return dm;
  return dm || ds;
}

/** Cinco años de búsqueda: suficiente para un 29 de febrero que cae en lunes. */
const HORIZONTE_MS = 5 * 366 * 24 * 3600 * 1000;

/**
 * El primer instante (minuto exacto, UTC) estrictamente posterior a `desde`
 * que calza con la expresión. Lanza ErrorCron si no hay ninguno en cinco
 * años (por ejemplo, `0 0 30 2 *`).
 */
export function siguienteEjecucion(expresion: ExpresionCron | string, desde: Date): Date {
  const c = typeof expresion === "string" ? parsearCron(expresion) : expresion;
  const t = new Date(desde.getTime());
  t.setUTCSeconds(0, 0);
  t.setUTCMinutes(t.getUTCMinutes() + 1);
  const limite = desde.getTime() + HORIZONTE_MS;

  while (t.getTime() <= limite) {
    if (!c.meses.has(t.getUTCMonth() + 1)) {
      t.setUTCMonth(t.getUTCMonth() + 1, 1);
      t.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!calzaDia(c, t)) {
      t.setUTCDate(t.getUTCDate() + 1);
      t.setUTCHours(0, 0, 0, 0);
      continue;
    }
    if (!c.horas.has(t.getUTCHours())) {
      t.setUTCHours(t.getUTCHours() + 1, 0, 0, 0);
      continue;
    }
    if (!c.minutos.has(t.getUTCMinutes())) {
      t.setUTCMinutes(t.getUTCMinutes() + 1, 0, 0);
      continue;
    }
    return t;
  }
  throw new ErrorCron(`«${c.fuente}» no tiene ejecuciones en los próximos cinco años`);
}
