/*
 * Medición (2026-10-20): en src/cron.ts se cambió la regla de día del mes /
 * día de la semana de OR a AND (`return dm && ds;` en calzaDia). Cayó 1 de 17
 * pruebas, la que debía («día del mes y día de la semana restringidos calzan
 * con cualquiera de los dos»); el corredor reportó 1 failed | 16 passed (17).
 * Revertido.
 */
import { describe, expect, it } from "vitest";
import { ErrorCron, parsearCron, siguienteEjecucion } from "../../src/cron.js";

const sig = (expr: string, desde: string) => siguienteEjecucion(expr, new Date(desde)).toISOString();

describe("parsearCron", () => {
  it("expande pasos, rangos y listas", () => {
    const c = parsearCron("*/15 9-17/4 1,15 * 1-5");
    expect([...c.minutos]).toEqual([0, 15, 30, 45]);
    expect([...c.horas]).toEqual([9, 13, 17]);
    expect([...c.diasMes]).toEqual([1, 15]);
    expect(c.meses.size).toBe(12);
    expect([...c.diasSemana]).toEqual([1, 2, 3, 4, 5]);
  });

  it("«a/n» va desde a hasta el final del campo", () => {
    expect([...parsearCron("50/5 * * * *").minutos]).toEqual([50, 55]);
  });

  it("el 7 del día de la semana es domingo, igual que el 0", () => {
    expect([...parsearCron("0 0 * * 7").diasSemana]).toEqual([0]);
  });

  it.each([
    ["* * * *", /5 campos/],
    ["60 * * * *", /fuera de 0-59/],
    ["* 24 * * *", /fuera de 0-23/],
    ["* * 0 * *", /fuera de 1-31/],
    ["* * * 13 *", /fuera de 1-12/],
    ["*/0 * * * *", /paso inválido/],
    ["5-1 * * * *", /al revés/],
    ["* * * JAN *", /no es un número/],
    ["1,,2 * * * *", /vacío/],
  ])("rechaza «%s»", (expr, mensaje) => {
    expect(() => parsearCron(expr)).toThrow(ErrorCron);
    expect(() => parsearCron(expr)).toThrow(mensaje);
  });
});

describe("siguienteEjecucion", () => {
  it("es estrictamente posterior y cae en minuto exacto", () => {
    expect(sig("* * * * *", "2026-01-01T10:00:00.000Z")).toBe("2026-01-01T10:01:00.000Z");
    expect(sig("* * * * *", "2026-01-01T10:00:30.500Z")).toBe("2026-01-01T10:01:00.000Z");
  });

  it("salta de hora, de día, de mes y de año", () => {
    expect(sig("30 9 * * *", "2026-03-10T09:30:00Z")).toBe("2026-03-11T09:30:00.000Z");
    expect(sig("0 0 1 * *", "2026-12-15T00:00:00Z")).toBe("2027-01-01T00:00:00.000Z");
  });

  it("día del mes y día de la semana restringidos calzan con cualquiera de los dos", () => {
    // 2026-10-20 es martes. «el 25 o cualquier lunes»: el próximo lunes es el 26,
    // pero el 25 llega antes.
    expect(sig("0 12 25 * 1", "2026-10-20T00:00:00Z")).toBe("2026-10-25T12:00:00.000Z");
    expect(sig("0 0 13 * 5", "2026-10-20T00:00:00Z")).toBe("2026-10-23T00:00:00.000Z");
  });

  it("encuentra un 29 de febrero", () => {
    expect(sig("0 0 29 2 *", "2026-03-01T00:00:00Z")).toBe("2028-02-29T00:00:00.000Z");
  });

  it("falla con una fecha imposible en vez de colgarse", () => {
    expect(() => sig("0 0 30 2 *", "2026-01-01T00:00:00Z")).toThrow(/cinco años/);
  });
});
