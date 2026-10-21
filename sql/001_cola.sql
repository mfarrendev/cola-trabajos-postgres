-- Migración de la cola. Idempotente: se puede correr más de una vez.
-- Las tablas van sin esquema calificado; el search_path decide dónde caen.

CREATE TABLE IF NOT EXISTS trabajos (
  id                 bigserial PRIMARY KEY,
  tipo               text        NOT NULL,
  payload            jsonb       NOT NULL DEFAULT '{}'::jsonb,
  estado             text        NOT NULL DEFAULT 'pendiente'
                     CHECK (estado IN ('pendiente', 'ejecutando', 'completado', 'muerto')),
  prioridad          integer     NOT NULL DEFAULT 0,
  run_at             timestamptz NOT NULL DEFAULT now(),
  intentos           integer     NOT NULL DEFAULT 0,
  max_intentos       integer     NOT NULL DEFAULT 5 CHECK (max_intentos >= 1),
  clave_idempotencia text,
  worker             text,
  lease_hasta        timestamptz,
  ultimo_error       text,
  creado_en          timestamptz NOT NULL DEFAULT now(),
  actualizado_en     timestamptz NOT NULL DEFAULT now(),
  terminado_en       timestamptz
);

-- La clave de idempotencia es única para siempre, no solo mientras el trabajo
-- está pendiente: encolar dos veces el mismo «enviar factura 881» no debe
-- enviarla dos veces aunque la primera ya haya terminado.
CREATE UNIQUE INDEX IF NOT EXISTS trabajos_clave_idempotencia
  ON trabajos (clave_idempotencia) WHERE clave_idempotencia IS NOT NULL;

-- Lo que miran los workers al tomar: solo pendientes, en el orden de toma.
CREATE INDEX IF NOT EXISTS trabajos_listos
  ON trabajos (prioridad DESC, run_at, id) WHERE estado = 'pendiente';

-- Leases vencidos de workers que murieron.
CREATE INDEX IF NOT EXISTS trabajos_leases
  ON trabajos (lease_hasta) WHERE estado = 'ejecutando';

CREATE INDEX IF NOT EXISTS trabajos_muertos
  ON trabajos (terminado_en DESC) WHERE estado = 'muerto';

CREATE TABLE IF NOT EXISTS cron_tareas (
  nombre       text PRIMARY KEY,
  expresion    text        NOT NULL,
  tipo         text        NOT NULL,
  payload      jsonb       NOT NULL DEFAULT '{}'::jsonb,
  max_intentos integer     NOT NULL DEFAULT 5,
  proxima      timestamptz NOT NULL,
  ultima       timestamptz
);
