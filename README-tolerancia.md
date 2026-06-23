# Tolerancia a Fallos

Segunda entrega. Se agregaron mecanismos para que el sistema tolere caídas de procesos y se recupere sin perder resultados.

---

## Chaos Monkey

Herramienta para testear tolerancia a fallos en forma activa. Es un container que cada N segundos elige un worker al azar y lo mata. Excluye por defecto a rabbitmq, al gateway, al cliente y al propio chaos monkey.

Se activa al generar el compose con `make generate` (opción `all`). Al final del asistente pregunta si querés incluirlo y con qué intervalo.

```
Add chaos monkey? [y/N]: y
Kill interval (seconds) [30]: 15
```

El container necesita acceso al socket de Docker, que se monta automáticamente en el compose generado.

---

## Watchdog

Container que monitorea los workers y los reinicia si se caen. Funciona en dos partes: los workers le mandan heartbeats por una cola de RabbitMQ, y el watchdog los escucha. Si un worker deja de mandar heartbeats por más de N segundos, el watchdog lo reinicia usando la API de Docker (docker-in-docker, montando `/var/run/docker.sock`).

La detección es a nivel de aplicación, no usando `docker inspect` ni nada del estilo. Si el proceso está colgado pero vivo, igual se detecta porque deja de mandar heartbeats.

Se activa igual que el chaos monkey, al generar el compose:

```
Add watchdog? [y/N]: y
Heartbeat timeout (seconds) [30]: 30
```

Cada worker manda su nombre de container en el heartbeat, que el watchdog usa para llamar a `container.start()`.

---

## Persistencia de estado

Cada worker guarda su estado en disco después de procesar cada mensaje. El estado se escribe en `/data/checkpoint.json` dentro de un volumen montado en `./data/<nombre-worker>/`. Si el worker se cae y el watchdog lo reinicia, arranca cargando ese checkpoint y retoma desde donde estaba.

La escritura es atómica: primero escribe a un archivo temporal y después hace un rename, para que no quede un JSON corrupto si se cae a la mitad de la escritura.

Lo que guarda cada worker:

- **filter** (filter_usd, q1_filter_amount, q4_filter_date, filter_q5_fmt, filter_q5_amount): el batch acumulado en memoria y los EOFs ya procesados por cliente.
- **split** (q4_split): los batches de origen y destino agrupados por routing key, y los EOFs ya procesados.
- **converter**: los EOFs ya procesados (las conversiones de moneda son stateless).
- **avg**: los acumuladores de suma y count por formato de pago y cliente.
- **split_date**: los batches del primer y segundo período separados por cliente.
- **avg_joiner**: los resultados de promedios recibidos, los contadores de EOF de ambas colas, y el estado de coordinación entre las dos colas. Los CSVs del spill a disco que ya tenía también persisten en el mismo volumen.
- **sg_detect**: los contadores de EOFs de origins y destinations. Los CSVs con los grafos de cuentas ya persistían en disco también.

Para limpiar todos los checkpoints: `make down` borra `./data/` junto con el resto.

---

## Deduplicación de mensajes

Problema: si un worker guarda el checkpoint y se cae antes de ackear el mensaje, RabbitMQ lo reencola y el worker lo volvería a procesar al reiniciar, duplicando el estado.

La solución es guardar el hash MD5 del cuerpo del último mensaje procesado dentro del mismo checkpoint. Al arrancar, si el primer mensaje que llega tiene el mismo hash que el guardado, es un reenvío de RabbitMQ y se descarta (ack sin procesar). Si el hash es distinto, es un mensaje nuevo y se procesa normalmente.

El orden en cada handler es siempre: procesar → guardar checkpoint → ackear. Así si se cae entre el checkpoint y el ack, el mensaje se reencola pero el hash lo identifica como duplicado.
