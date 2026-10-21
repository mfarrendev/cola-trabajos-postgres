import { describe, expect, it } from "vitest";
import { esperaReintento } from "../../src/backoff.js";

const op = { baseMs: 1000, maxMs: 60_000 };

describe("esperaReintento", () => {
  it("se duplica por intento y el azar solo mueve la mitad superior", () => {
    expect(esperaReintento(1, op, () => 0)).toBe(500);
    expect(esperaReintento(1, op, () => 1)).toBe(1000);
    expect(esperaReintento(4, op, () => 0)).toBe(4000);
    expect(esperaReintento(4, op, () => 1)).toBe(8000);
  });

  it("respeta el tope aunque el intento sea enorme", () => {
    expect(esperaReintento(500, op, () => 1)).toBe(60_000);
    expect(esperaReintento(500, op, () => 0)).toBe(30_000);
  });

  it("dispersa: mil reintentos con azar real no salen todos iguales", () => {
    const valores = new Set(Array.from({ length: 1000 }, () => esperaReintento(3, op)));
    expect(valores.size).toBeGreaterThan(100);
    for (const v of valores) {
      expect(v).toBeGreaterThanOrEqual(2000);
      expect(v).toBeLessThanOrEqual(4000);
    }
  });

  it("rechaza intentos que no son enteros positivos", () => {
    expect(() => esperaReintento(0, op)).toThrow(RangeError);
    expect(() => esperaReintento(1.5, op)).toThrow(RangeError);
  });
});
