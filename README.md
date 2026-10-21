# cola-trabajos-postgres

Cola de trabajos en segundo plano sobre Postgres, sin Redis. Librería en
TypeScript con el driver `pg`.

## El problema

Una aplicación que ya tiene Postgres suele necesitar trabajos en segundo
plano: enviar correos, generar un PDF, llamar a una API lenta, un resumen a
las 8 de la mañana. Lo habitual es sumar Redis y una librería de colas, que
es un servicio más que operar, respaldar y vigilar, y que además deja los
trabajos fuera de la transacción de la base: se puede confirmar el pedido y
perder el correo, o encolar el correo de un pedido que después hizo rollback.

Esta librería guarda los trabajos en una tabla de la misma base. Lo que se
pide de una cola seria tiene que seguir valiendo:

- que dos workers no ejecuten el mismo trabajo;
- que si un worker muere a mitad, el trabajo no se pierda;
- que un fallo se reintente con espera creciente y, si sigue fallando, quede
  apartado con su error para que alguien lo mire;
- que una tarea programada corra una vez por instante aunque haya varios
  workers revisando el reloj;
- que apagar un worker no corte un trabajo a la mitad.

## Diseño y decisiones

**Una tabla, cuatro estados.** `trabajos` tiene `pendiente`, `ejecutando`,
`completado` y `muerto`. La migración está en `sql/001_cola.sql` y es
idempotente.

**Tomar con `FOR UPDATE SKIP LOCKED`.** Un worker toma trabajos con una sola
sentencia: un `SELECT ... ORDER BY prioridad DESC, run_at, id LIMIT n FOR
UPDATE SKIP LOCKED` dentro de un `UPDATE` que los marca como suyos. Dos
workers que toman a la vez se saltan las filas que el otro bloqueó en vez de
esperarlas o repetirlas. La prueba de integración lo comprueba con 4
procesos y 1000 trabajos; sin el `SKIP LOCKED` la misma prueba registró 1573
ejecuciones para esos 1000 trabajos.

**Leases en vez de "en proceso" para siempre.** Al tomar un trabajo el
worker recibe un lease (`lease_hasta`, 30 s por omisión) y lo renueva con un
heartbeat cada un tercio de ese tiempo. Si el proceso muere, nadie renueva, y
cuando el lease vence otro worker lo toma con `intentos + 1` y un
`ultimo_error` que dice qué worker no terminó. Completar, fallar y renovar
llevan `WHERE worker = <yo> AND estado = 'ejecutando'`: un worker que se
quedó colgado y despierta tarde no puede pisar el resultado del que lo
reemplazó. Si el heartbeat descubre que perdió el lease, aborta el
`AbortSignal` que recibe el manejador.

**El reloj es el de Postgres.** Todos los plazos (`run_at`, leases, backoff)
se calculan con `now()` de la base. Con varias máquinas, cada una con su
reloj, un lease podría vencer antes para un worker que para otro.

**Reintentos con backoff exponencial y "jitter igual".** La espera antes del
reintento `k` es `min(max, base * 2^(k-1))`, de la que la mitad es fija y la
otra mitad es azar. Con jitter completo un décimo reintento podría salir casi
inmediato; así nunca baja de la mitad de lo que toca y los workers que
fallaron juntos igual se dispersan. Al agotar `max_intentos` el trabajo pasa a
`muerto` con el último error (el stack, recortado a 8000 caracteres).

**Un trabajo que mata a su worker también muere.** Si el lease vence en el
último intento, el trabajo pasa a `muerto` en vez de volver a la cola. Si no,
un trabajo que tumba el proceso (memoria, una dependencia nativa) se
reintentaría para siempre.

**Idempotencia por índice único.** `clave_idempotencia` tiene un índice
único parcial y el `INSERT` usa `ON CONFLICT DO NOTHING`. La clave vale para
siempre, no solo mientras el trabajo está pendiente: encolar dos veces
"enviar factura 881" no la envía dos veces aunque la primera ya haya
terminado. Encolar devuelve `{ id, creado }`.

**Cron sin duplicados, sin elegir líder.** Cada worker revisa las tareas
vencidas cada pocos segundos. La ejecución de la tarea `resumen` a las
08:00 se encola con la clave `cron:resumen:2026-10-21T08:00:00.000Z`, así que
aunque tres workers vean la misma tarea vencida, el índice único deja entrar
una. Después `proxima` se avanza con un `UPDATE` condicionado al valor leído.
No hace falta advisory lock ni un worker especial. Si el sistema estuvo
apagado, se encola una sola ejecución atrasada y se salta a la siguiente
futura; ponerse al día con todas casi nunca es lo que se quiere.

**Parser cron propio.** Cinco campos (`minuto hora día mes día-semana`), con
`*`, números, rangos, listas y pasos (`*/15`, `9-17/2`, `5/10`), en UTC. Si
día del mes y día de la semana están restringidos los dos, basta con que
calce uno, como en el cron clásico. No acepta nombres (`MON`) ni `@daily`: los
rechaza con un error. Una fecha imposible como `0 0 30 2 *` falla en vez de
colgarse buscando.

**Apagado limpio.** `worker.detener()` deja de tomar trabajos y espera a los
que tiene; no los aborta. `apagarConSenales([worker])` lo conecta a SIGTERM y
SIGINT; una segunda señal sale de inmediato.

**Cada operación es una sentencia.** Ninguna operación necesita una
conexión dedicada ni una transacción explícita, así que todo funciona sobre
un `pg.Pool` y no queda estado a medias si el proceso muere entre dos
llamadas. La interfaz que pide la librería es solo `query(texto, parámetros)`,
que cumplen `pg.Pool`, `pg.Client` y PGlite; por eso las pruebas unitarias
corren en memoria con el mismo SQL.

## Uso

```bash
npm install
docker run -d --name pg-cola-demo -e POSTGRES_PASSWORD=local -p 127.0.0.1:55511:5432 postgres:17-alpine
```

```ts
import pg from "pg";
import { Cola, Worker, apagarConSenales, iniciarPanel, migrar } from "cola-trabajos-postgres";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await migrar(pool);

const cola = new Cola(pool);
await cola.encolar("enviar-correo", { para: "ana@example.com" }, {
  prioridad: 5,                       // mayor, antes
  retrasoMs: 60_000,                  // o runAt: new Date(...)
  maxIntentos: 3,
  claveIdempotencia: "bienvenida:ana@example.com",
});
await cola.programar("resumen-diario", "0 8 * * 1-5", "resumen", {});

const worker = new Worker(pool, {
  concurrencia: 4,
  leaseMs: 30_000,
  backoff: { baseMs: 1_000, maxMs: 600_000 },
  manejadores: {
    "enviar-correo": async (payload, { trabajo, signal }) => { /* ... */ },
    resumen: async () => { /* ... */ },
  },
}).iniciar();

const { url } = await iniciarPanel(pool, { puerto: 8080 }); // 127.0.0.1 por omisión
apagarConSenales([worker]);
```

El panel tiene tres rutas: `GET /` (HTML con conteos por estado y los últimos
muertos con su error), `GET /api/estado` (lo mismo en JSON) y
`POST /reintentar/:id` (devuelve un muerto a la cola con los intentos en
cero).

Scripts:

| Script | Qué hace |
| --- | --- |
| `npm test` | unitarias (PGlite) e integración (Postgres 17; se saltan con aviso si no hay servidor) |
| `npm run test:unit` / `npm run test:integracion` | por separado |
| `npm run typecheck` | `tsc --noEmit` |
| `npm run build` | compila a `dist/` |
| `npm run ejemplo` | correos simulados con worker, cron y panel |
| `npm run bench` | trabajos por segundo contra Postgres |

`COLA_PG_URL` cambia el Postgres de pruebas, ejemplo y benchmark.

## Salida real

Pruebas (Postgres 17 en Docker levantado). Las líneas de SIGTERM son de los
procesos hijos de la integración, que se apagan limpio:

```
 RUN  v5.0.5 /Users/tamarafarren/Desktop/proyectos-github/cola-trabajos-postgres

SIGTERM: no se toman trabajos nuevos; esperando los que están en curso
SIGTERM: no se toman trabajos nuevos; esperando los que están en curso
SIGTERM: no se toman trabajos nuevos; esperando los que están en curso
SIGTERM: no se toman trabajos nuevos; esperando los que están en curso
SIGTERM: no se toman trabajos nuevos; esperando los que están en curso

 Test Files  7 passed (7)
      Tests  55 passed (55)
   Start at  21:28:57
   Duration  13.56s (tests 98%, import 2%)
```

Sin Postgres, la integración se salta y lo dice:

```
[integración] AVISO: no hay Postgres en postgres://x:***@127.0.0.1:1/x; se saltan las pruebas de integración.
  docker run -d --name pg-cola-demo -e POSTGRES_PASSWORD=local -p 127.0.0.1:55511:5432 postgres:17-alpine
 Test Files  1 skipped (1)
      Tests  5 skipped (5)
```

Las pruebas de integración cubren: 4 workers en 4 procesos procesan 1000
trabajos y cada uno se ejecuta exactamente una vez; un proceso hijo muere con
SIGKILL a mitad de un trabajo y otro worker lo retoma cuando vence el lease
(intento 2, con el error `lease vencido: el worker hijo-<pid> no terminó`);
un hijo que recibe SIGTERM termina su trabajo, no toma el siguiente y sale con
código 0; 50 encolados simultáneos con la misma clave dejan uno; y una tarea
cron revisada por 3 workers se encola una vez por instante.

Ejemplo (`DURACION_MS=4000 PUERTO=8091 npm run ejemplo`), primera corrida:

```
encolar ana@example.com: id 1
encolar luis@example.com: id 2
encolar rebota@example.com: id 3
panel en http://127.0.0.1:8091
enviado a urgente@example.com («Alerta», intento 1)
enviado a ana@example.com («Bienvenida», intento 1)
enviado a luis@example.com («Bienvenida», intento 1)
SIGTERM: no se toman trabajos nuevos; esperando los que están en curso
estado final: { pendiente: 0, ejecutando: 0, completado: 3, muerto: 1 }
```

El panel en esa corrida (`/api/estado`, resumido):

```
{'pendiente': 0, 'ejecutando': 0, 'completado': 3, 'muerto': 1}
[('3', 3, 'Error: el servidor de rebota@example.com rechazó el mensaje')]
```

Segunda corrida sobre la misma base: las bienvenidas no se duplican.

```
encolar ana@example.com: id 1 (ya existía, no se duplica)
encolar luis@example.com: id 2 (ya existía, no se duplica)
encolar rebota@example.com: id 3 (ya existía, no se duplica)
panel en http://127.0.0.1:8091
enviado a urgente@example.com («Alerta», intento 1)
SIGTERM: no se toman trabajos nuevos; esperando los que están en curso
estado final: { pendiente: 0, ejecutando: 0, completado: 4, muerto: 1 }
```

## Benchmark

`npm run bench`: 5000 trabajos con un manejador vacío (mide la cola, no el
trabajo), Postgres 17.11 en Docker y Node 24.11.1 en la misma máquina, un
MacBook con Apple M1 Pro. Cuatro corridas seguidas:

```
Postgres 17.11, Node v24.11.1, 5000 trabajos
encolar (de a uno, 20 en paralelo): 0.59 s, 8504 trabajos/s
procesar (4 workers x concurrencia 10): 3.63 s, 1377 trabajos/s

encolar (de a uno, 20 en paralelo): 0.71 s, 7066 trabajos/s
procesar (4 workers x concurrencia 10): 1.72 s, 2904 trabajos/s

encolar (de a uno, 20 en paralelo): 0.63 s, 7944 trabajos/s
procesar (4 workers x concurrencia 10): 1.51 s, 3305 trabajos/s

encolar (de a uno, 20 en paralelo): 0.80 s, 6275 trabajos/s
procesar (4 workers x concurrencia 10): 1.55 s, 3220 trabajos/s
```

Del orden de 7000 encolados y 3000 trabajos procesados por segundo; la primera
corrida, en frío, procesó la mitad. Cada toma son dos sentencias (el barrido de
leases vencidos y el `UPDATE ... SKIP LOCKED`, que trae varios trabajos de una
vez) y cada trabajo suma su `completar`, y el número depende mucho de la latencia a Postgres: con la base en
otra máquina será menor. Son mediciones de una sola máquina, no una
comparación con otras colas.

## Limitaciones

- **Al menos una vez, no exactamente una vez.** Si un worker termina el
  trabajo y muere antes de marcarlo completado, el lease vence y el trabajo
  se repite. Los manejadores con efectos externos deben ser idempotentes. Lo
  mismo si un manejador ignora el `signal` y sigue después de perder el lease.
- **Sondeo, no `LISTEN/NOTIFY`.** Un worker sin trabajo revisa cada
  `sondeoMs` (500 ms por omisión): la latencia mínima de un trabajo nuevo es
  esa, y cada worker ocioso hace una consulta por intervalo.
- **La tabla crece.** Los completados y muertos se quedan; no hay purga
  automática. Con mucho volumen hace falta un `DELETE` periódico (o particionar
  por fecha) y vigilar el autovacuum, porque cada trabajo genera varias
  versiones de fila.
- **Las claves de idempotencia son permanentes** mientras la fila exista;
  purgar un trabajo libera su clave.
- **Cron en UTC, sin zonas horarias** ni nombres de meses o días.
- **Se pierde a lo sumo una ejecución atrasada por tarea**: tras un apagón se
  corre una, no todas las que faltaron.
- **El panel no tiene autenticación.** Escucha en 127.0.0.1 y rechaza POST de
  otro origen, pero exponerlo pide ponerlo detrás de algo que autentique.
- **`encolar` no participa de una transacción ajena** salvo que se le pase
  el mismo cliente con el que se abrió (`new Cola(client)` dentro de un
  `BEGIN` propio); con un pool, cada llamada va por su cuenta.
- La migración es un solo archivo; no hay sistema de versiones de esquema.

## Licencia

MIT.
