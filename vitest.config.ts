import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Las pruebas de integración comparten un Postgres; cada archivo usa su
    // propio esquema, pero se corren de a uno para que el conteo de tiempos
    // del benchmark y de los leases no compita por CPU.
    fileParallelism: false,
  },
});
