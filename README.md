# Money Laundering Analysis

Alumnos: 
- Baez, Thiago Fernando - tbaez@fi.uba.ar - Padrón 110703
- Llanos Pontaut, Valentina - vllanos@fi.uba.ar - Padrón 104413

## Datasets

Bajar los datasets de [Kaggle - IBM Transactions for Anti-Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) y poner los archivos en el directorio `datasets/`.


## Expected Output

Bajar los [archivos de salida esperada](https://drive.google.com/drive/folders/1bOl9gDLcdXP1tUwwhLBHqjyLvgr9zjMQ?usp=sharing) y guardarlos dentro de la carpeta `expected` en el repositorio. Se deben guardar las carpetas tal cual aparecen:

```
money-laundering-analysis/
└── expected/
    ├── HI-Medium_Trans.csv/
    └── HI-Small_Trans.csv/
```
---

## Uso rápido

1. make generate      -> genera un docker-compose con los workers deseados
2. make switch-query  -> elige qué compose usar
3. make up            -> levanta el sistema y sigue los logs
4. make down          -> baja todo y limpia

# para correr todas las queries de una:
make compare          -> compara manualmente contra los expected files.
```

---

## Comandos

### make generate

Lanza un asistente interactivo para elegir la query y configurar los workers. Cada parámetro muestra su valor por defecto entre corchetes; presioná Enter para aceptarlo o escribí otro valor.

```
$ make generate
=== Generador de docker-compose ===
Opciones: 1, 3, 4, 5, all
Que compose queres generar? [1/3/4/5/all]: 5

[ Query 5 — parametros ]
  n_clients                    [1]:
  Archivos disponibles en datasets/:
    1) HI-Medium_Trans.csv
    2) HI-Small_Trans.csv
  input_file client 0          [HI-Medium_Trans.csv]:
  filter_fmt    (workers)      [7]:
  converter     (workers)      [3]:
  filter_amount (workers)      [2]:
  batch_size                   [10000]:
  -- Chaos Monkey --
  agregar chaos monkey?        [s/n, default=n]:
  -- Watchdog --
  agregar watchdog?            [s/n, default=n]:
  output file                  [docker-compose-q5.yaml]:
```

El asistente lista los datasets disponibles en `datasets/` y permite elegirlos por número. El nombre del archivo de salida también se puede cambiar.

Chaos Monkey: si se activa, se agrega un container que mata workers al azar cada N segundos. Excluye rabbitmq y el propio chaos monkey. Podes elegir si excluir o no los clientes y gateway para pruebas generales.

Watchdog: si se activa, se despliegan 3 instancias de watchdog en anillo y se inyecta `WATCHDOG_COUNT` a todos los workers. Se puede configurar el timeout de heartbeat (segundos sin latido para considerar un worker caído).

La opción `all` genera un compose que corre las 4 queries a la vez con un solo cliente y permite configurar cada query por separado antes de elegir chaos monkey y watchdog.

Los scripts también se pueden correr directamente desde `generate/`:

| Query | Script |
| ----- | ------ |
| Q1    | generate/generate_compose_q1.py  |
| Q3    | generate/generate_compose_q3.py  |
| Q4    | generate/generate_compose_q4.py  |
| Q5    | generate/generate_compose_q5.py  |
| Todas | generate/generate_compose_all.py |

---

### make switch-query

Lista los .yaml de la raíz y pide elegir uno. El seleccionado queda en .compose y es el que usan make up, make down y make logs.

```
$ make switch-query
  1) docker-compose-q1.yaml
  2) docker-compose-q3.yaml
  3) docker-compose-q4.yaml
  4) docker-compose-q5.yaml
Seleccionar compose [1-4]: 4
Usando: docker-compose-q4.yaml
```

Si nunca se corrió make switch-query, busca docker-compose.yaml por defecto.

---

### make which

Indica cuál es el actual docker-compose utilizado para desplegar el sistema.


---

### make up / make down / make logs

make up construye las imágenes, levanta los contenedores y sigue los logs. Cuando el cliente termina escribe los resultados en output/client0/query{N}/.

make down para los contenedores, borra volúmenes e imágenes y limpia output/ y los temporales de Q3. Usa sudo internamente para borrar esos directorios.

make logs muestra los logs del compose activo sin seguirlos.

---

### make compare

make compare abre el script de comparación para elegir qué query verificar. Compara count.csv y tx.csv contra expected/ sin importar el orden de las filas. Para Q4 las cuentas intermediarias también se comparan sin orden.

---

### make lint

```
make lint 
```

---

### Heartbeats

Cada worker que debe ser monitoreado corre un HeartbeatSender en un hilo daemon. Cada 3 segundos publica un JSON

```json
{"container": CONTAINER_NAME, "ts": <timestamp>}
```

al exchange heartbeat_exchange de Rabbit usando routing keys: watchdog_0, watchdog_1, watchdog_2. Es decir, el mismo heartbeat llega a los 3 watchdogs simultáneamente.

Del lado del watchdog, cada instancia escucha en su propia routing key. Cuando recibe un heartbeat, actualiza last_seen[container_name]. Cada 5 segundos , el watchdog que sea líder itera sobre ese diccionario y compara el timestamp actual contra el último heartbeat. Si el gap supera HEARTBEAT_TIMEOUT segundos, llama a docker start <container>.

El worker manda a todos los watchdogs (no solo al líder) porque no sabe quién es el líder en cada momento. Así, si el líder cae y otro toma el rol, ya tiene el historial de heartbeats actualizado y puede detectar workers caídos sin período de arranque en frío.

Un detalle importante: si el worker no se puede conectar a Rabbit (por ejemplo, justo después de reiniciarse), el HeartbeatSender reintenta la conexión automáticamente con un sleep de 5 segundos entre intentos, sin matar al worker.

### Watchdog 

El sistema incluye un mecanismo de watchdog para detectar y recuperar workers caídos automáticamente.

Arquitectura: Anillo de Watchdogs

Se despliegan 3 instancias de watchdog (watchdog_0, watchdog_1, watchdog_2) organizadas en una forma de anillo. Cada watchdog monitorea al watchdog anterior (watchdog_i monitorea a watchdog_(i-1) % 3), de modo que si uno cae, el siguiente lo levanta. Esto garantiza que el sistema de monitoreo en sí también sea tolerante a fallos.

```txt
watchdog_2 ←── watchdog_0
    │                ↑
    └──→ watchdog_1 ─┘
```

Líder dinámico

Solo el watchdog con el ID más bajo que esté vivo actúa como líder y monitorea los workers. Si el líder se cae, el siguiente watchdog con menor ID toma el liderazgo. Cada watchdog evalúa si existe algún peer con ID menor que haya enviado heartbeat recientemente. Si no existe ninguno, se declara líder.

Heartbeats de workers

Cada worker envía periódicamente un heartbeat (cada 3 segundos por defecto) al exchange de RabbitMQ heartbeat_exchange. El mensaje incluye el nombre del container y un timestamp. El heartbeat se envía a todos los watchdogs simultáneamente mediante routing keys individuales (watchdog_0, watchdog_1, watchdog_2), de modo que cualquiera de ellos puede tomar el liderazgo con información actualizada.

Si un worker no envía heartbeat durante más de HEARTBEAT_TIMEOUT segundos, el líder lo detecta e intenta levantarlo via Docker API (docker start <container>).

Heartbeats entre watchdogs (peers)

Los watchdogs también se envían heartbeats entre sí a través del exchange watchdog_exchange, con routing keys peer_0, peer_1, peer_2. Si un watchdog detecta que el peer que monitorea no envió heartbeat en más de WATCHDOG_TIMEOUT segundos, lo reinicia.

### chaos monkey

Al generar con `make generate` y elegir `all`, al final pregunta si querés agregar el chaos monkey y con qué intervalo (en segundos). Si lo activás, se agrega un container que mata workers al azar cada N segundos para testear tolerancia a fallos.

Por defecto excluye rabbitmq, el propio chaos monkey y los clientes. El resto de los workers son candidatos.

Para que funcione el container necesita acceso al socket de docker, que se monta automáticamente cuando se genera el compose con esta opción.

## Queries

[Notebook con querys en nuestra versión](https://www.kaggle.com/code/valenpontaut/money-laundering-analysis/notebook?scriptVersionId=322848864)


**Q1** — Transacciones en USD menores a $50  
Workers: filter_usd, filter_amount

**Q3** — Promedio de transacciones por formato de pago en dos períodos  
Workers: filter_usd, split_date, avg, avg_joiner

**Q4** — Detección de cuentas con formato scather-gatter entre origen y destino  
Workers: filter_usd, filter_date, split, og_detect, dt_detect, sg_detect


**Q5** — Transacciones Wire/ACH en septiembre 2022 con monto < $1 USD  
Workers: filter_fmt, converter, filter_amount
