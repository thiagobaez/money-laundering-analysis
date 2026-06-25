# Análisis de Tolerancia a Fallos — Money Laundering Analysis

## Índice

1. [Worker Filter](#1-worker-filter)
   - [Contexto](#contexto)
   - [Escenario 1: Caída antes de `_flush_batch_to_disk`](#escenario-1-caída-durante-procesamiento-de-mensaje-de-datos-antes-de-_flush_batch_to_disk)
   - [Escenario 2: Caída entre `_flush_batch_to_disk` y `_save_checkpoint`](#escenario-2-caída-después-de-_flush_batch_to_disk-pero-antes-de-_save_checkpoint)
   - [Escenario 3: Caída entre `_save_checkpoint` y `ack`](#escenario-3-caída-después-de-_save_checkpoint-pero-antes-de-ack)
   - [Escenario 4: Caída en EOF antes de `_save_checkpoint`](#escenario-4-caída-durante-procesamiento-de-eof-antes-de-_save_checkpoint)
   - [Escenario 5: Caída en EOF después de `_save_checkpoint`](#escenario-5-caída-después-de-_save_checkpoint-en-eof-antes-de-ack)
   - [Escenario 6: Caída en `_send_disk_rows` antes de `os.remove`](#escenario-6-caída-dentro-de-_send_disk_rows-antes-de-osremove)
   - [Escenario 7: Caída durante la escritura atómica en `_flush_batch_to_disk`](#escenario-7-caída-durante-la-escritura-atómica-en-_flush_batch_to_disk)

2. [Worker Avg](#2-worker-avg)
   - [Contexto](#contexto-1)
   - [Escenario 1: Caída antes de `_save_checkpoint`](#escenario-1-caída-durante-procesamiento-de-mensaje-de-datos-antes-de-_save_checkpoint-1)
   - [Escenario 2: Caída entre `_save_checkpoint` y `ack`](#escenario-2-caída-después-de-_save_checkpoint-pero-antes-de-ack-1)
   - [Escenario 3: Caída en EOF antes de `_save_checkpoint`](#escenario-3-caída-durante-procesamiento-de-eof-antes-de-_save_checkpoint-1)
   - [Escenario 4: Caída en EOF después de `_save_checkpoint`](#escenario-4-caída-después-de-_save_checkpoint-en-eof-antes-de-ack-1)

3. [Worker AvgJoiner](#3-worker-avgjoiner)
   - [Contexto](#contexto-2)
   - [Escenario 1: Caída en datos antes de `_save_checkpoint`](#escenario-1-caída-durante-procesamiento-de-datos-antes-de-_save_checkpoint-2)
   - [Escenario 2: Caída en datos después de `_save_checkpoint`](#escenario-2-caída-después-de-_save_checkpoint-antes-de-ack-datos-2)
   - [Escenario 3: Caída en EOF de segundo período](#escenario-3-caída-durante-procesamiento-de-eof-de-segundo-período-antes-de-_save_checkpoint)
   - [Escenario 4: Caída en EOF de segundo período después de `_save_checkpoint`](#escenario-4-caída-después-de-_save_checkpoint-en-eof-de-segundo-período-antes-de-ack)
   - [Escenario 5: Caída en batch de promedios](#escenario-5-caída-durante-procesamiento-de-batch-de-promedios-antes-de-_save_checkpoint)
   - [Escenario 6: Caída en batch de promedios después de `_save_checkpoint`](#escenario-6-caída-después-de-_save_checkpoint-en-batch-de-promedios-antes-de-ack)
   - [Escenario 7: Caída en AVG_EOF antes de `_save_checkpoint`](#escenario-7-caída-durante-procesamiento-de-avg_eof-antes-de-_save_checkpoint)
   - [Escenario 8: Caída en AVG_EOF después de `_save_checkpoint`](#escenario-8-caída-después-de-_save_checkpoint-en-avg_eof-antes-de-ack)
   - [Escenario 9: Caída en `_flush_to_output` entre la reescritura del spill y el envío a output](#escenario-9-caída-en-_flush_to_output-después-de-reescribir-o-borrar-el-spill-pero-antes-de-enviar-el-output)
   - [Escenario 10: Caída en `_flush_all_spill_to_output` entre el borrado del spill y el envío a output](#escenario-10-caída-en-_flush_all_spill_to_output-después-de-borrar-el-spill-pero-antes-de-enviar-el-output-al-eof)

4. [Worker SplitDate](#4-worker-splitdate)
   - [Contexto](#contexto-3)
   - [Escenario 1: Caída en datos antes de `_save_checkpoint`](#escenario-1-caída-durante-procesamiento-de-datos-antes-de-_save_checkpoint-3)
   - [Escenario 2: Caída en datos después de `_save_checkpoint`](#escenario-2-caída-después-de-_save_checkpoint-antes-de-ack-datos-3)
   - [Escenario 3: Caída en EOF antes de `_save_checkpoint`](#escenario-3-caída-durante-procesamiento-de-eof-antes-de-_save_checkpoint-3)
   - [Escenario 4: Caída en EOF después de `_save_checkpoint`](#escenario-4-caída-después-de-_save_checkpoint-en-eof-antes-de-ack-3)

5. [Worker Split](#5-worker-split)
   - [Contexto](#contexto-4)
   - [Escenario 1: Caída en datos antes de `_save_checkpoint`](#escenario-1-caída-durante-procesamiento-de-datos-antes-de-_save_checkpoint-4)
   - [Escenario 2: Caída en datos después de `_save_checkpoint`](#escenario-2-caída-después-de-_save_checkpoint-antes-de-ack-datos-4)
   - [Escenario 3: Caída en EOF antes de `_save_checkpoint`](#escenario-3-caída-durante-_on_eof-antes-de-_save_checkpoint)
   - [Escenario 4: Caída en EOF después de `_save_checkpoint`](#escenario-4-caída-después-de-_save_checkpoint-en-eof-antes-de-ack-4)

6. [Worker OgDetect](#6-worker-ogdetect)
   - [Contexto](#contexto-5)
   - [Escenario 1: Caída en datos antes de `ack`](#escenario-1-caída-durante-procesamiento-de-datos-antes-de-ack)
   - [Escenario 2: Caída durante `_on_eof_message`](#escenario-2-caída-durante-_on_eof_message-antes-de-ack)
   - [Escenario 3: Caída después del paso 5, antes de `ack`](#escenario-3-caída-después-del-paso-5-completo-antes-de-ack)

7. [Worker DtDetect](#7-worker-dtdetect)

8. [Worker SgDetect](#8-worker-sgdetect)
   - [Contexto](#contexto-7)
   - [Escenario 1: Caída en datos antes de `_save_checkpoint`](#escenario-1-caída-durante-procesamiento-de-datos-origins-o-destinations-antes-de-_save_checkpoint)
   - [Escenario 2: Caída en datos después de `_save_checkpoint`](#escenario-2-caída-después-de-_save_checkpoint-en-datos-antes-de-ack)
   - [Escenario 3: Caída en EOF antes de `_save_checkpoint`](#escenario-3-caída-durante-procesamiento-de-eof-origins-o-destinations-antes-de-_save_checkpoint)
   - [Escenario 4: Caída en EOF después de `_save_checkpoint`](#escenario-4-caída-después-de-_save_checkpoint-en-eof-antes-de-ack-8)
   - [Escenario 5: Caída dentro de `_check_and_emit`](#escenario-5-caída-dentro-de-_check_and_emit-después-de-_emit_results-pero-antes-del-output_queuesend-final)

9. [Worker Converter](#9-worker-converter)
   - [Contexto](#contexto-8)
   - [Escenario 1: Caída en datos antes de `_save_checkpoint`](#escenario-1-caída-durante-procesamiento-de-datos-antes-de-_save_checkpoint-9)
   - [Escenario 2: Caída en datos después de `_save_checkpoint`](#escenario-2-caída-después-de-_save_checkpoint-antes-de-ack-datos-9)
   - [Escenario 3: Caída en EOF antes de `_save_checkpoint`](#escenario-3-caída-durante-procesamiento-de-eof-antes-de-_save_checkpoint-9)
   - [Escenario 4: Caída en EOF después de `_save_checkpoint`](#escenario-4-caída-después-de-_save_checkpoint-en-eof-antes-de-ack-9)

---

## 1. Worker Filter

### Contexto

El `Filter` es un worker single-threaded que:

- Consume mensajes de una cola o exchange de RabbitMQ.
- Filtra transacciones por criterios configurables (moneda, monto, fecha, formato de pago).
- Acumula filas filtradas en memoria (`self.batches`) y las persiste a disco periódicamente.
- Manda filas downstream cuando se acumula `BATCH_SIZE` filas en disco.
- Coordina el fin de procesamiento por cliente via un protocolo de contador de EOF circulante (`eof_seen` + `counter`).

Estado persistido:

<<<<<<< HEAD
- Disco (`DATA_DIR/<client_id>/rows.csv`): filas filtradas acumuladas, pendientes de envío downstream.
- Checkpoint (`DATA_DIR/checkpoint.json`): `last_msg_hash` + `eof_seen`.
=======
- **Disco** (`DATA_DIR/<client_id>/rows.csv`): filas filtradas acumuladas, pendientes de envío downstream. La escritura es atómica: `_flush_batch_to_disk` lee el contenido existente, escribe todo (existente + nuevo) a `rows.csv.tmp` y reemplaza el archivo con `os.replace`. Esto evita que una caída a mitad de escritura deje el CSV con una fila truncada o corrupta (riesgo que existía con el modo `"a"` usado anteriormente).
- **Checkpoint** (`DATA_DIR/checkpoint.json`): `last_msg_hash` + `eof_seen`.
>>>>>>> 1b7e442551cdc438ea4cb9ecd7817d7afc7c3ad7

---

### Escenario 1: Caída durante procesamiento de mensaje de datos, antes de `_flush_batch_to_disk`

Cuándo ocurre: el worker muere mientras está iterando sobre las transacciones del mensaje y acumulando en `self.batches`, antes de llegar a `_flush_batch_to_disk`.

Estado al morir:
- `self.batches` tiene filas en memoria no persistidas.
- `checkpoint.json` tiene el `last_msg_hash` del mensaje anterior.
- El archivo de disco no cambió.

Al reiniciar:
- `_load_checkpoint` carga el hash anterior.
- Si hay archivos en disco de batches previos, los manda downstream.
- RabbitMQ redelivera el mensaje (no fue ackeado).
- El hash del mensaje redelivered no coincide con `last_msg_hash` → se reprocesa correctamente.

Resultado: correcto. No se pierden filas, no hay duplicados.

---

### Escenario 2: Caída después de `_flush_batch_to_disk` pero antes de `_save_checkpoint`

Cuándo ocurre: las filas ya se escribieron a disco pero el checkpoint todavía no se actualizó con el nuevo `last_msg_hash`.

Estado al morir:
- Disco tiene las filas nuevas.
- `checkpoint.json` tiene el hash del mensaje anterior.

Al reiniciar:
- `_load_checkpoint` encuentra el archivo de disco → manda las filas downstream.
- RabbitMQ redelivera el mensaje.
- El hash no coincide con `last_msg_hash` → se reprocesa → se vuelven a acumular las mismas filas → se vuelven a escribir a disco → se mandan downstream de nuevo.

Resultado: duplicado de filas. Las mismas filas se mandan dos veces al siguiente worker.

La ventana sin cobertura es la caída entre `_flush_batch_to_disk` y `_save_checkpoint`.

```python
self._flush_batch_to_disk(client_id)
self._last_msg_hash = h
self._save_checkpoint()
if self._disk_counts.get(client_id, 0) >= BATCH_SIZE:
    self._send_disk_rows(client_id)
ack()
```

---

### Escenario 3: Caída después de `_save_checkpoint` pero antes de `ack`

Cuándo ocurre: todo está persistido pero el `ack` no llegó a RabbitMQ.

Estado al morir:
- Disco tiene las filas (o ya se mandaron si se alcanzó `BATCH_SIZE`).
- `checkpoint.json` tiene el nuevo `last_msg_hash`.

Al reiniciar:
- Si había filas en disco, `_load_checkpoint` las manda downstream.
- RabbitMQ redelivera el mensaje.
- El hash coincide con `last_msg_hash` → descartado sin reprocesar.

Resultado: correcto. El `last_msg_hash` protege exactamente este caso.

---

### Escenario 4: Caída durante procesamiento de EOF, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere dentro de `_on_eof`, después de mandar filas downstream y el EOF al siguiente eslabón o reencolarlo, pero antes de que se guarde el checkpoint.

Estado al morir:
- `eof_seen` en disco todavía no tiene este `client_id`.
- Las filas del disco ya se mandaron y el archivo se borró.
- El EOF downstream (o reencola del contador) ya se mandó.

Al reiniciar:
- `_load_checkpoint` no encuentra archivos en disco (ya se borraron).
- RabbitMQ redelivera el EOF.

Resultado:
- **Caso cubierto:** si el worker murió después de persistir `_last_msg_hash` pero antes del `ack` → el EOF redelivered coincide con `_last_msg_hash` → descartado sin reprocesar. 
- **Caso no cubierto:** si el worker murió después de `_send_output` pero antes de persistir `_last_msg_hash` → el hash no está en el checkpoint → el EOF se manda downstream de nuevo.

La ventana sin cobertura es la caída entre `_send_output` y `_save_checkpoint`.

```python
def _on_eof(self, client_id, counter, msg_hash):
    self._flush_batch_to_disk(client_id)
    self._send_disk_rows(client_id)
    if client_id not in self.eof_seen:
        self.eof_seen.add(client_id)
        if counter > 1:
            self.input_queue.send(message_protocol.internal
                .serialize([client_id, "EOF", counter - 1]))
        else:
            self._send_output(message_protocol.internal
                .serialize([client_id]))
            self._last_msg_hash = msg_hash
            self._save_checkpoint()
            self.eof_seen.discard(client_id)

h = checkpoint.msg_hash(message)
if h == self._last_msg_hash:
    ack()
    return
if self._is_eof(fields):
    self._on_eof(client_id, self._get_eof_counter(fields), h)
    self._save_checkpoint()
    ack()
    return
```

---

### Escenario 5: Caída después de `_save_checkpoint` en EOF, antes de `ack`

Cuándo ocurre: el checkpoint se guardó con `eof_seen` actualizado, pero el `ack` no llegó.

Al reiniciar:
- RabbitMQ redelivera el EOF.
- `client_id` sí está en `eof_seen` → entra al `else` → reencola el EOF con el mismo `counter` sin decrementar.

Resultado: correcto. El `eof_seen` protege este caso. El EOF sigue circulando con el mismo `counter` hasta que alguna instancia que no lo vio lo procese.

---

### Escenario 6: Caída dentro de `_send_disk_rows`, antes de `os.remove`

Cuándo ocurre: el worker muere mientras está enviando las filas al siguiente worker (`_send_batch`), antes de llegar a `os.remove`.

Al reiniciar:
- El archivo de disco sigue intacto (el `os.remove` nunca se ejecutó).
- `_load_checkpoint` encuentra el archivo y vuelve a mandar las mismas filas downstream.
- RabbitMQ no redelivera nada — el mensaje que disparó el send ya fue ackeado en una corrida anterior (este escenario ocurre en `_load_checkpoint`, no en `_on_message`).

Resultado: duplicado de filas en el siguiente worker. No hay pérdida de datos.

La ventana sin cobertura es la caída entre `_send_batch` y `os.remove` en `_load_checkpoint`.

```python
def _send_disk_rows(self, client_id):
    ...
    with open(path, "r", newline="") as f:
        rows = list(csv.reader(f))
    self._send_batch(client_id, rows)
    os.remove(path)
    self._disk_counts[client_id] = 0
```

---

### Escenario 7: Caída durante la escritura atómica en `_flush_batch_to_disk`

**Cuándo ocurre:** el worker muere mientras `_flush_batch_to_disk` escribe el archivo temporal `rows.csv.tmp` (lee las filas existentes + agrega las nuevas), en cualquier punto antes de que `os.replace` lo renombre a `rows.csv`.

**Estado al morir:**
- `rows.csv.tmp` puede quedar a medio escribir (incompleto o corrupto).
- `rows.csv` permanece intacto con el contenido previo al mensaje en curso — `os.replace` nunca llegó a ejecutarse, así que el archivo real nunca está en un estado parcial.
- El checkpoint no se actualizó (`_save_checkpoint` se llama después de `_flush_batch_to_disk`).

**Al reiniciar:**
- `_load_checkpoint` solo mira `rows.csv`; el `.tmp` huérfano se ignora y queda en el directorio del cliente hasta que el próximo flush lo sobreescriba.
- RabbitMQ redelivera el mensaje (no fue ackeado).
- El hash no coincide con `last_msg_hash` → se reprocesa → `_flush_batch_to_disk` vuelve a leer `rows.csv` íntegro y reescribe el `.tmp` correctamente.

**Resultado:** correcto. El patrón `.tmp` + `os.replace` garantiza que `rows.csv` siempre está en uno de dos estados consistentes — el contenido anterior completo, o el nuevo contenido completo — nunca a medio escribir. Esto reemplaza el modo `"a"` (append) que se usaba antes, donde una caída en medio de un `write()` podía dejar una fila truncada y corromper las lecturas posteriores del CSV. El archivo `.tmp` huérfano no afecta la correctitud.

```python
def _flush_batch_to_disk(self, client_id):
    rows = self.batches.pop(client_id, [])
    if not rows:
        return
    path = self._file_path(client_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    existing_rows = []
    if os.path.exists(path):
        with open(path, "r", newline="") as f:
            existing_rows = list(csv.reader(f))
    with open(tmp_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(existing_rows)
        writer.writerows(rows)
    os.replace(tmp_path, path)
```

---

## 2. Worker Avg

### Contexto

`Avg` es un worker single-threaded que:

- Consume batches de transacciones del primer período (post-filtro).
- Acumula sumas y conteos por `(client_id, payment_format)` para calcular el promedio.
- Al recibir un EOF, calcula los promedios finales y los manda a todas las instancias de `avg_joiner`.
- No tiene protocolo de contador circulante. Recibe un único EOF por cliente (coordinado upstream).

Estado persistido en checkpoint:
- `last_msg_hash`: hash del último mensaje de datos procesado.
- `accum`: acumulador completo de sumas/conteos por cliente y formato de pago.

---

### Escenario 1: Caída durante procesamiento de mensaje de datos, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere mientras acumula datos en `self.accum`, antes de guardar el checkpoint.

Estado al morir:
- `self.accum` en memoria tiene datos nuevos no persistidos.
- `checkpoint.json` tiene el `last_msg_hash` y `accum` del mensaje anterior.

Al reiniciar:
- `_load_checkpoint` restaura `accum` al estado del mensaje anterior.
- RabbitMQ redelivera el mensaje.
- El hash no coincide con `last_msg_hash` → se reprocesa → los datos se acumulan correctamente de nuevo.

Resultado: correcto. No hay pérdida ni duplicado, se reprocesa el último mensaje.

---

### Escenario 2: Caída después de `_save_checkpoint` pero antes de `ack`

Cuándo ocurre: el checkpoint se guardó correctamente pero el `ack` no llegó a RabbitMQ.

Al reiniciar:
- `_load_checkpoint` restaura `accum` y `last_msg_hash` correctos.
- RabbitMQ redelivera el mensaje.
- El hash coincide con `last_msg_hash` → descartado.

Resultado: correcto. El `last_msg_hash` protege exactamente este caso.

---

### Escenario 3: Caída durante procesamiento de EOF, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere dentro de `_flush`, después de mandar los promedios y el EOF a `avg_joiner`, pero antes de guardar el checkpoint.

Estado al morir:
- `accum.pop(client_id)` ya ejecutó en memoria.
- Los promedios y el EOF ya se mandaron a las colas de `avg_joiner`.
- `checkpoint.json` todavía tiene `accum` con los datos del cliente y el `last_msg_hash` del mensaje anterior (no del EOF).

Al reiniciar:
- `_load_checkpoint` restaura `accum` con los datos del cliente.
- RabbitMQ redelivera el EOF.
- El hash del EOF no coincide con `last_msg_hash` (que tiene el hash del último dato, no del EOF) → no se descarta → entra a `_flush` → manda promedios y EOF de nuevo.

Resultado: promedios y EOF duplicados llegan a `avg_joiner`. Los promedios duplicados son idempotentes — `avg_joiner` sobreescribe el mismo valor sin acumular, y `_flush_to_output` no hace nada si el archivo de spill ya fue procesado. El EOF duplicado de la misma fuente queda mitigado por `avg_eof_seen` — `avg_joiner` lo descarta sin incrementar el contador. Resultado final: correcto, aunque con trabajo redundante.

```python
if self._is_eof(fields):
    self._flush(client_id)
    self._last_msg_hash = h
    self._save_checkpoint()
    ack()
    return
```

---

### Escenario 4: Caída después de `_save_checkpoint` en EOF, antes de `ack`

Cuándo ocurre: el checkpoint se guardó (con `accum` ya sin el cliente, post-pop) pero el `ack` no llegó.

Al reiniciar:
- `_load_checkpoint` restaura `accum` sin ese `client_id` y `last_msg_hash` con el hash del EOF.
- RabbitMQ redelivera el EOF.
- El hash del EOF coincide con `last_msg_hash` → descartado.

Resultado: correcto. El hash del EOF está persistido en el checkpoint, así que el redelivery se descarta sin reprocesar.

---

## 3. Worker AvgJoiner

### Contexto

`AvgJoiner` es un worker multi-threaded (dos threads de consumo) que:

- Recibe transacciones del segundo período por la cola `second_period_queue` (compartida entre las `AVG_JOINER_AMOUNT` instancias).
- Recibe promedios por formato de pago desde las instancias de `avg` por su propia cola dedicada (`avg_joiner_N`).
- Para cada transacción del segundo período: si ya tiene el promedio de su formato de pago, evalúa si pasó el umbral y la manda a output; si no, la spillea a disco esperando que llegue el promedio.
- Cuando llegan todos los EOFs esperados de ambas fuentes (`sp_eof_done` y `avg_eof` para el mismo `client_id`), flushea todo lo que quedó en disco a output y manda el EOF final downstream.
- Usa un protocolo de contador circulante para coordinar el EOF de segundo período entre las `AVG_JOINER_AMOUNT` instancias.

Estado persistido en checkpoint:
- `last_sp_hash`: hash del último mensaje de datos del segundo período procesado (no aplica a EOFs de segundo período por el protocolo de contador circulante).
- `last_avg_hash`: hash del último mensaje de avg procesado.
- `eof_seen`: set de `client_id`s cuyo EOF de segundo período ya procesó esta instancia (usado para el protocolo de contador).
- `sp_eof_done`: set de `client_id`s cuyo lado de segundo período ya completó en esta instancia.
- `avg_eof`: set de `client_id`s cuyo lado de avg ya completó en esta instancia.
- `avg_eof_seen`: `{client_id: set(avg_ids)}` de instancias de avg que ya mandaron su EOF para cada cliente.
- `avg_results`: promedios recibidos por `(client_id, payment_format)`.

<<<<<<< HEAD
Estado persistido en disco (fuera del checkpoint):
- `DATA_DIR/<client_id>/<payment_format>.csv`: transacciones del segundo período spilleadas a disco, esperando que llegue el promedio de ese formato de pago.
=======
**Estado persistido en disco (fuera del checkpoint):**
- `DATA_DIR/<client_id>/spill.csv`: un único archivo por cliente (en vez de uno por `payment_format` como antes) con las transacciones del segundo período spilleadas a disco de todos los formatos de pago, usando el `payment_format` como primera columna de cada fila. Las escrituras (`_flush_spill_to_disk`, el rewrite parcial de `_flush_to_output`, el borrado de `_flush_all_spill_to_output`) son atómicas vía `.tmp` + `os.replace`, por lo que una caída a mitad de escritura nunca deja `spill.csv` corrupto o truncado.
>>>>>>> 1b7e442551cdc438ea4cb9ecd7817d7afc7c3ad7

---

### Escenario 1: Caída durante procesamiento de datos, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere mientras procesa filas del segundo período, antes de guardar el checkpoint con el nuevo `last_sp_hash`.

Estado al morir:
- Algunas filas pueden estar en `spill_batches` en memoria (no en disco todavía) o ya escritas a disco.
- `last_sp_hash` en checkpoint tiene el hash del mensaje anterior.

Al reiniciar:
- `_load_checkpoint` restaura el estado anterior.
- RabbitMQ redelivera el mensaje.
- Hash no coincide → reprocesa.
- Si las filas ya estaban en disco (spill), se pueden procesar dos veces cuando llegue el avg.
- Si estaban solo en memoria, se pierden y se reescriben correctamente en el reproceso.

Resultado: potencial duplicado de filas si el spill ya llegó a disco antes del crash.

<<<<<<< HEAD
La ventana sin cobertura es la caída entre `_flush_all_spill_batches` y `_save_checkpoint`.
=======
> **Ventana de falla:** caída entre `_flush_spill_to_disk` y `_save_checkpoint` — puede causar envíos duplicados.
>>>>>>> 1b7e442551cdc438ea4cb9ecd7817d7afc7c3ad7

---

### Escenario 2: Caída después de `_save_checkpoint`, antes de `ack` (datos)

Cuándo ocurre: checkpoint guardado correctamente, `ack` no llegó.

Al reiniciar:
- Hash coincide con `last_sp_hash` → descartado.

Resultado: correcto.

---

### Escenario 3: Caída durante procesamiento de EOF de segundo período, antes de `_save_checkpoint`

Cuándo ocurre: el worker procesó el EOF (agregó a `sp_eof_done`, llamó `_try_send_eof`) pero murió antes de guardar el checkpoint.

Estado al morir:
- `sp_eof_done` en memoria tiene el `client_id`.
- `eof_seen` en memoria tiene el `client_id`.
- Checkpoint tiene el estado anterior, sin `sp_eof_done` ni `eof_seen` actualizados.
- Si `_try_send_eof` llegó a ejecutarse, el EOF final ya se mandó downstream. Los archivos de spill ya fueron borrados.

Al reiniciar:
- `_load_checkpoint` restaura `sp_eof_done` y `eof_seen` sin ese `client_id`.
- RabbitMQ redelivera el EOF de segundo período.
- `client_id` no está en `eof_seen` → entra como primera vez → agrega a `sp_eof_done` → llama `_try_send_eof` de nuevo.
- Si `avg_eof` también tiene el `client_id` → `_try_send_eof` ejecuta → archivos de spill ya no existen → no se duplican resultados → pero el EOF downstream se manda de nuevo.

Resultado: EOF duplicado downstream. Los resultados no se duplican porque los archivos de spill ya fueron borrados en la primera ejecución.

La ventana sin cobertura es la caída entre `_try_send_eof` y `_save_checkpoint`.

```python
self._send_output(message_protocol.internal.serialize(
    [client_id, QUERY_NUMBER, "EOF", worker_id]))
self._last_sp_hash = msg_hash
self._save_checkpoint()
```

---

### Escenario 4: Caída después de `_save_checkpoint` en EOF de segundo período, antes de `ack`

Cuándo ocurre: checkpoint guardado con `sp_eof_done` y `eof_seen` actualizados, `ack` no llegó.

Al reiniciar:
- RabbitMQ redelivera el EOF.
- `client_id` está en `eof_seen` → entra al `else` → reencola sin decrementar.

Resultado: correcto. `eof_seen` protege este caso.

---

### Escenario 5: Caída durante procesamiento de batch de promedios, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere mientras procesa promedios de avg (actualizando `avg_results` y llamando `_flush_to_output`), antes de guardar el checkpoint.

Estado al morir:
- `avg_results` en memoria tiene los promedios nuevos.
- `spill.csv` puede haber sido reescrito (sin las filas de este `payment_format`) o borrado por `_flush_to_output`.
- Checkpoint tiene `avg_results` del estado anterior y `last_avg_hash` del mensaje anterior.

<<<<<<< HEAD
Al reiniciar:
- `avg_results` se restaura al estado anterior (sin los promedios nuevos).
=======
**Al reiniciar:**
- `avg_results` se restaura al estado anterior (sin el promedio nuevo).
>>>>>>> 1b7e442551cdc438ea4cb9ecd7817d7afc7c3ad7
- RabbitMQ redelivera el batch de promedios.
- Hash no coincide → reprocesa → `avg_results` se actualiza → `_flush_to_output` se llama de nuevo.
- Si `spill.csv` todavía tiene filas de ese `payment_format` (el crash fue antes de la reescritura atómica) → se procesan de nuevo → comportamiento idéntico a un primer intento, sin duplicado ni pérdida.
- Si `spill.csv` ya fue reescrito/borrado en el intento anterior (el crash fue **después** de la reescritura) → esas filas ya no están en el archivo → `_flush_to_output` no las vuelve a encontrar → nunca se reintenta enviarlas a output.

<<<<<<< HEAD
Resultado: potencial duplicado de filas si el archivo de spill no fue borrado antes del crash.

La ventana sin cobertura es la caída entre `_flush_to_output` y `_save_checkpoint`, cuando el archivo de spill fue procesado pero el checkpoint no llegó a guardarse.
=======
**Resultado:** con el código actual, `_flush_to_output` reescribe/borra `spill.csv` (de forma atómica) **antes** de mandar `output_batch` a output (ver Escenario 9). Esto invierte el riesgo respecto al diseño anterior (un archivo por `payment_format`, que se borraba *después* de enviar el output): antes el peor caso era un duplicado; ahora el peor caso es una **pérdida silenciosa de filas** si el crash ocurre entre la reescritura del archivo y el envío a output. Si el crash ocurre antes de la reescritura, el reproceso es seguro.

> **Ventana de falla:** caída entre la reescritura/borrado atómico de `spill.csv` y `_save_checkpoint` — puede causar pérdida de filas (no duplicado) si el crash cae específicamente entre la reescritura del spill y el envío a output dentro de `_flush_to_output` (ver Escenario 9 para el detalle de esa sub-ventana).
>>>>>>> 1b7e442551cdc438ea4cb9ecd7817d7afc7c3ad7

---

### Escenario 6: Caída después de `_save_checkpoint` en batch de promedios, antes de `ack`

Cuándo ocurre: checkpoint guardado con `avg_results` y `last_avg_hash` actualizados, `ack` no llegó.

Al reiniciar:
- Hash coincide con `last_avg_hash` → descartado.

Resultado: correcto.

---

### Escenario 7: Caída durante procesamiento de AVG_EOF, antes de `_save_checkpoint`

Cuándo ocurre: el worker procesó el AVG_EOF (agregó a `avg_eof_seen`, quizás llamó `_try_send_eof`) pero murió antes de guardar el checkpoint.

Estado al morir:
- `avg_eof_seen` en memoria tiene el `avg_id`.
- Si fue el último AVG_EOF esperado, `avg_eof` tiene el `client_id` y `_try_send_eof` se ejecutó.
- Checkpoint no refleja ninguno de estos cambios.

Al reiniciar:
- `avg_eof_seen` se restaura sin ese `avg_id`.
- RabbitMQ redelivera el AVG_EOF.
- `avg_id` no está en `avg_eof_seen` → se procesa de nuevo → si completa `AVG_AMOUNT` → llama `_try_send_eof` de nuevo.
- Si el lado de segundo período no completó todavía (`sp_eof_done` no tiene el `client_id`) → `_try_send_eof` retorna sin hacer nada. 
- Si el lado de segundo período ya completó y el checkpoint lo persistió (`sp_eof_done` tiene el `client_id`) → `_try_send_eof` ejecuta de nuevo → EOF duplicado downstream.

Resultado: EOF duplicado downstream solo si ambos lados completaron pero el checkpoint del AVG_EOF no llegó a guardarse. Los resultados no se duplican porque los archivos de spill ya fueron borrados en la primera ejecución. La mitigación de `avg_eof_seen` no alcanza para este caso porque el checkpoint no llegó a persistirla.

La ventana sin cobertura es la caída dentro de `_try_send_eof` antes de `_save_checkpoint`.

---

### Escenario 8: Caída después de `_save_checkpoint` en AVG_EOF, antes de `ack`

Cuándo ocurre: checkpoint guardado con `avg_eof_seen` actualizado, `ack` no llegó.

Al reiniciar:
- RabbitMQ redelivera el AVG_EOF.
- `avg_id` está en `avg_eof_seen` → descartado.

Resultado: correcto. `avg_eof_seen` protege exactamente este caso.

---

### Escenario 9: Caída en `_flush_to_output`, después de reescribir o borrar el spill pero antes de enviar el output

**Cuándo ocurre:** dentro de `_flush_to_output`, el worker ya filtró las filas de `spill.csv` que corresponden al `payment_format` recibido, ya reescribió (o borró, si no quedan filas de otros formatos) `spill.csv` de forma atómica, pero todavía no llamó a `_append_output_rows` para mandar `output_batch` a la cola de salida.

```python
with spill_lock:
    ...
    if remaining_rows:
        ...
        os.replace(tmp_path, path)
    else:
        os.remove(path)
        ...
# <-- ventana de falla: spill.csv ya actualizado, output_batch todavía no enviado
self._append_output_rows(client_id, output_batch)
```

**Estado al morir:**
- `spill.csv` ya no contiene las filas de este `payment_format` (fueron filtradas y excluidas de la reescritura, o el archivo entero se borró).
- `output_batch` (las transacciones que pasaban el umbral del promedio) solo existe en memoria del proceso muerto — nunca llegó a `self.output_queue`.
- El checkpoint todavía no se guardó (eso pasa más adelante, en `_on_avg_message`).

**Al reiniciar:**
- RabbitMQ redelivera el mensaje de avg (no fue ackeado).
- El hash no coincide con `last_avg_hash` → se reprocesa → `_flush_to_output` se llama de nuevo para el mismo `payment_format`.
- Pero `spill.csv` ya no tiene esas filas (se quitaron en el intento anterior) → el reproceso no las encuentra → `output_batch` queda vacío para esas filas.

**Resultado:** **pérdida de datos.** Las transacciones que debían pasar a output para ese `payment_format` se pierden silenciosamente — no se duplican (el archivo ya no las tiene) pero tampoco se vuelven a enviar. Este riesgo es nuevo respecto al diseño anterior (un archivo por `client_id`+`payment_format`), donde `_flush_to_output` enviaba el output **antes** de borrar el archivo, así que el peor caso era un duplicado, nunca una pérdida. Para eliminar esta ventana habría que invertir el orden (enviar el output antes de actualizar `spill.csv`), aceptando duplicados en su lugar — consistente con el patrón "at-least-once" usado en el resto del sistema.

---

### Escenario 10: Caída en `_flush_all_spill_to_output`, después de borrar el spill pero antes de enviar el output al EOF

**Cuándo ocurre:** dentro de `_flush_all_spill_to_output` (llamado por `_try_send_eof` cuando ambos lados — segundo período y avg — completaron para un cliente), el worker ya leyó y filtró todas las filas restantes de `spill.csv`, ya borró el archivo y su directorio, pero todavía no llamó a `_append_output_rows` con el batch final.

```python
with spill_lock:
    ...
    os.remove(path)
    try:
        os.rmdir(os.path.dirname(path))
    except OSError:
        pass
# <-- ventana de falla: spill.csv ya borrado, output_batch todavía no enviado
self._append_output_rows(client_id, output_batch)
```

**Estado al morir:**
- `spill.csv` ya no existe — toda la información de las filas pendientes de ese cliente solo vivía en `output_batch`, en memoria.
- `_try_send_eof` todavía no llegó a mandar el EOF final ni a guardar el checkpoint (esos pasos ocurren después, en el mismo método, fuera de esta función).

**Al reiniciar:**
- RabbitMQ redelivera el mensaje que disparó `_try_send_eof` (EOF de segundo período o AVG_EOF, según cuál haya sido el último en completar).
- Se reprocesa: como ambos lados siguen marcados como completos (o se vuelven a completar), `_try_send_eof` se ejecuta de nuevo → llama a `_flush_all_spill_to_output` de nuevo.
- `spill.csv` ya no existe → la función retorna en el primer `if not os.path.exists(path): return` → nunca se reconstruye `output_batch` ni se reintenta el envío.

**Resultado:** **pérdida de datos**, potencialmente más grave que el Escenario 9 porque ocurre en el flush final de cierre por cliente — todas las filas que quedaron spilleadas hasta el EOF y no se pudieron enviar antes del crash se pierden sin posibilidad de recuperación. Es la ventana de falla más riesgosa introducida por el cambio a escritura atómica en este worker, aunque su duración es muy corta (dos instrucciones de Python dentro del mismo proceso).

---

## 4. Worker SplitDate

### Contexto

`SplitDate` es un worker single-threaded que:

- Consume batches de transacciones filtradas (post `filter_usd`).
- Clasifica cada transacción por rango de fecha: primer período → acumula en `first_batches` y manda a las colas de `avg` (distribuidas por hash de `payment_format`); segundo período → acumula en `second_batches` y manda a `second_period_queue`.
- Al recibir un EOF, flushea todos los batches pendientes y coordina el EOF downstream usando el protocolo de contador circulante (`eof_seen` + `counter`).
- No tiene spill a disco — los batches pendientes viven en el checkpoint.

Estado persistido en checkpoint:
- `last_msg_hash`: hash del último mensaje de datos procesado.
- `eof_seen`: set de `client_id`s cuyo EOF ya procesó esta instancia (para el protocolo de contador).
- `first_batches`: batches pendientes del primer período, por `(client_id, queue_idx)`.
- `second_batches`: batches pendientes del segundo período, por `client_id`.

---

### Escenario 1: Caída durante procesamiento de datos, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere mientras acumula transacciones en `first_batches`/`second_batches` o mientras flushea un batch completo, antes de guardar el checkpoint.

Estado al morir:
- `first_batches`/`second_batches` en memoria pueden tener filas nuevas no persistidas, o batches ya mandados downstream (si se alcanzó `BATCH_SIZE` y se flusheó).
- Checkpoint tiene el estado del mensaje anterior.

Al reiniciar:
- `_load_checkpoint` restaura `first_batches`/`second_batches` al estado anterior.
- RabbitMQ redelivera el mensaje.
- Hash no coincide → reprocesa correctamente.
- Si un batch ya se mandó downstream antes del crash, al reprocesar se manda de nuevo → duplicado posible.

Resultado: potencial duplicado de filas si un batch se flusheó downstream antes del crash.

La ventana sin cobertura es la caída entre `_flush_first_batch`/`_flush_second_batch` y `_save_checkpoint`.

---

### Escenario 2: Caída después de `_save_checkpoint`, antes de `ack` (datos)

Cuándo ocurre: checkpoint guardado correctamente, `ack` no llegó.

Al reiniciar: hash coincide con `last_msg_hash` → descartado.

Resultado: correcto.

---

### Escenario 3: Caída durante procesamiento de EOF, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere dentro de `_on_eof` — después de flushear batches y mandar el EOF downstream (o reencolarlo), pero antes de guardar el checkpoint.

Estado al morir:
- `eof_seen` en memoria tiene el `client_id` (o no, dependiendo de cuándo murió).
- El EOF downstream (o reencola del contador) ya se mandó.
- Checkpoint no refleja estos cambios — `eof_seen` sigue sin ese `client_id`, y `first_batches`/`second_batches` pueden tener los datos que ya se flushearon.

Al reiniciar:
- `_load_checkpoint` restaura `eof_seen` sin ese `client_id` y `first_batches`/`second_batches` con datos que ya se mandaron.
- RabbitMQ redelivera el EOF.

Resultado:
- **Caso cubierto (`counter=1`):** el hash coincide con `_last_msg_hash` → descartado. 
- **Caso no cubierto (`counter > 1` o `else`):** el hash no está en `_last_msg_hash` → entra como primera vez → `_flush_all_batches` manda los batches de nuevo — el checkpoint los tiene guardados porque no llegó a actualizarse con los batches vacíos post-flush → duplicado de filas downstream.

La ventana sin cobertura es la caída entre `_flush_all_batches`/envío downstream y `_save_checkpoint` (solo para `counter > 1` o rama `else`).

```python
if self._is_eof(fields):
    self._flush_all_batches(client_id)
    self._on_eof(client_id, self._get_eof_counter(fields), h)
    self._save_checkpoint()
    ack()
    return
```

---

### Escenario 4: Caída después de `_save_checkpoint` en EOF, antes de `ack`

Cuándo ocurre: checkpoint guardado con `eof_seen` actualizado (y `first_batches`/`second_batches` ya vacíos post-flush), `ack` no llegó.

Al reiniciar:
- RabbitMQ redelivera el EOF.
- `client_id` está en `eof_seen` → entra al `else` → reencola sin decrementar.

Resultado: correcto. `eof_seen` protege este caso.

---

## 5. Worker Split

### Contexto

`Split` es un worker single-threaded que:

- Comparte la cola de entrada entre `SPLIT_AMOUNT=3` instancias.
- Por cada transacción, la rutea simultáneamente a `og_detect` (por hash de `from_account`) y a `dt_detect` (por hash de `to_account`).
- Acumula filas en `_origin_batches` y `_dest_batches` y flushea cuando llegan a `BATCH_SIZE`.
- Coordina el fin de procesamiento via contador circulante (`eof_received_by_client` + `counter`).

Estado persistido en checkpoint:
- `last_msg_hash`: hash del último mensaje de datos procesado.
- `eof_received_by_client`: lista de `client_id`s cuyo EOF ya procesó esta instancia.
- `origin_batches`: batches acumulados por `(client_id, routing_key)` hacia `og_detect`.
- `dest_batches`: batches acumulados por `(client_id, routing_key)` hacia `dt_detect`.

---

### Escenario 1: Caída durante procesamiento de datos, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere mientras itera sobre las filas del batch y las acumula en `_origin_batches`/`_dest_batches`, o durante un flush al alcanzar `BATCH_SIZE` mid-loop.

Estado al morir:
- Algunos batches pueden haberse enviado downstream (si se alcanzó `BATCH_SIZE` durante el loop).
- `_origin_batches`/`_dest_batches` en checkpoint tienen el estado del mensaje anterior.
- `last_msg_hash` apunta al mensaje anterior.

Al reiniciar:
- `_load_checkpoint` restaura batches al estado previo.
- RabbitMQ redelivera el mensaje.
- Hash no coincide → reprocesa → los batches flusheados antes del crash se envían de nuevo.

Resultado: duplicado posible de filas en `og_detect`/`dt_detect` si un batch se flusheó antes del crash.

---

### Escenario 2: Caída después de `_save_checkpoint`, antes de `ack` (datos)

Resultado: correcto. Hash coincide → descartado.

---

### Escenario 3: Caída durante `_on_eof`, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere dentro de `_on_eof` — después de `_flush_all_batches` y la acción del contador (re-encolar o enviar EOF downstream) — pero antes de `_save_checkpoint()`.

Estado al morir:
- Los batches pendientes fueron enviados downstream y removidos de memoria.
- El counter fue decrementado y re-encolado (o el EOF downstream fue enviado).
- Checkpoint aún tiene: `eof_received_by_client` sin este `client_id`, y los batches previos al flush.

Al reiniciar:
- `_load_checkpoint` restaura batches (pre-flush) y `eof_received_by_client` sin `client_id`.
- RabbitMQ redelivera el EOF.
- `client_id` not in `eof_received_by_client` → entra como "primera vez".
- `_flush_all_batches` encuentra los batches en el checkpoint → los envía downstream de nuevo → duplicado de filas.
- Si `counter > 1`: re-encola `[client_id, "EOF", counter-1]` de nuevo → hay dos copias del mensaje en la cola → el counter se decrementa dos veces → dos EOFs llegarán a `og_detect`/`dt_detect`.
- Si `counter == 1`: envía EOF downstream de nuevo → EOF duplicado hacia `og_detect` y `dt_detect`.

Resultado: duplicado de filas y de EOF. Es el caso análogo al escenario 3 de `filter`/`splitDate`. No hay protección sin separar el checkpoint de batches del de `eof_received_by_client`.

---

### Escenario 4: Caída después de `_save_checkpoint` en EOF, antes de `ack`

Estado al morir:
- Checkpoint tiene `eof_received_by_client` con `client_id` y batches ya vacíos.
- La acción del contador ya ejecutó.

Al reiniciar:
- RabbitMQ redelivera el EOF.
- `client_id` sí está en `eof_received_by_client` → rama `else` → re-encola con el mismo `counter`.

Observación: dado que ningún worker `split` remueve `client_id` de `eof_received_by_client` después de enviar el EOF downstream (`counter==1`), todas las instancias tienen `client_id` en su lista. El mensaje re-encolado rebotará indefinidamente entre instancias sin que ninguna lo procese como "primera vez". No genera duplicados downstream pero deja un mensaje huérfano circulando en la cola de entrada de `split`.

Resultado: el EOF downstream no se duplica (correcto), pero queda un mensaje huérfano en la cola.

---

## 6. Worker OgDetect

### Contexto

`OgDetect` es un worker single-threaded que:

- Consume de su propio routing key del exchange `q4_split_exchange` (uno por instancia).
- Escribe pares `from_account\tto_account` a `/data/{client_id}/log.bin` mediante `_append_log`: lee el contenido completo existente, lo escribe junto con las filas nuevas a `log.bin.tmp`, y reemplaza el original con `os.replace` (escritura atómica) antes de cada `ack()`. Ya no usa un file handle persistente en modo `"ab"` con buffer de Python — cada llamada reescribe el archivo entero.
- Al recibir EOF: lee `log.bin` completo, construye un `account_map` con sets, envía cuentas con >= `MIN_DESTINATIONS` destinos a `sg_detect` (por hash de la cuenta), borra `client_dir`, envía EOF a todos los routing keys de `sg_detect`.
- No tiene checkpoint. El único estado persistido es el archivo `log.bin`.

---

### Escenario 1: Caída durante procesamiento de datos, antes de `ack`

**Cuándo ocurre:** el worker muere mientras `_append_log` escribe `log.bin`, ya sea durante la escritura del archivo temporal `log.bin.tmp` (antes de `os.replace`) o justo después de que `os.replace` lo haya hecho efectivo.

**Estado al morir:**
- **Crash antes de `os.replace`:** `log.bin` permanece intacto con el contenido anterior al mensaje en curso; el `.tmp` parcial se ignora y se sobreescribe en el próximo intento. No hay riesgo de fila truncada en el archivo real, a diferencia del modo `"ab"` + buffer de Python anterior.
- **Crash después de `os.replace`:** `log.bin` ya contiene las filas nuevas del mensaje, de forma completa y consistente.

**Al reiniciar:**
- RabbitMQ redelivera el mensaje (no fue ackeado).
- `_append_log` se ejecuta de nuevo, leyendo el `log.bin` actual y agregando las mismas filas nuevas.
- Si el crash fue antes del `replace`: el resultado final es idéntico al de un primer intento exitoso, sin duplicados.
- Si el crash fue después del `replace`: las filas del mensaje quedan escritas dos veces en `log.bin` (duplicado de líneas).
- En `_on_eof_message`, `account_map[from_acc]` es un `set` → `add(to_acc)` es idempotente → los pares duplicados no afectan el resultado final.

**Resultado:** correcto en ambos sub-casos. El patrón de escritura atómica elimina el riesgo de truncamiento/corrupción a mitad de escritura; el único duplicado posible (líneas repetidas en `log.bin`) sigue protegido por la idempotencia del `set` en `_on_eof_message`, igual que con el diseño anterior.

---

### Escenario 2: Caída durante `_on_eof_message`, antes de `ack`

El flujo de `_on_eof_message` es:
1. Cerrar `log.bin`
2. Leer `log.bin` → construir `account_map` con sets
3. Enviar cuentas calificadas a `sg_detect` (routing key por hash de cuenta)
4. `shutil.rmtree(client_dir)`
5. Loop: enviar EOF a cada routing key de `sg_detect`
→ `ack()` en `_on_message`

Sub-escenario A: crash durante el paso 3 (enviando cuentas calificadas)
- `client_dir` aún existe, `log.bin` intacto.
- On restart: re-lee `log.bin` → re-envía todas las cuentas a `sg_detect`.
- `sg_detect` escribe con `open(..., "w")` → sobreescritura idempotente.

Resultado: correcto.

Sub-escenario B: crash durante el paso 5 (loop de EOFs a `sg_detect`, incompleto)
- `client_dir` ya fue eliminado (paso 4 completó).
- Algunos `sg_detect` workers recibieron el EOF, otros no.
- On restart: RabbitMQ redelivera el EOF de `og_detect` → `_on_eof_message` → no hay `log.bin` → `account_map` vacío → no envía cuentas → envía EOF a todos los routing keys de `sg_detect` de nuevo.
- Los `sg_detect` que ya tenían el EOF reciben un segundo → `origins_eofs[client_id]` sube por encima de `NUM_OG_WORKERS`.
- Si el conteo supera `NUM_OG_WORKERS` antes de que los otros `og_detect` manden su EOF → `_check_and_emit` se puede disparar con `destinations` incompleto, o dispararse dos veces si `destinations` ya completó.

Resultado: duplicado de EOFs en `sg_detect` puede causar `_check_and_emit` múltiple → salida duplicada. Este es el riesgo más alto de toda la query 4.

---

### Escenario 3: Caída después del paso 5 completo, antes de `ack`

- Todo enviado: cuentas y EOFs a todos los `sg_detect`, `client_dir` eliminado.
- On restart: RabbitMQ redelivera → `_on_eof_message` → no hay `log.bin` → envía EOFs a todos los `sg_detect` de nuevo.

Resultado: mismo riesgo que Sub-escenario B. Los EOFs se duplican en `sg_detect`.

---

## 7. Worker DtDetect

Estructura idéntica a `OgDetect` pero con los campos invertidos (`to_account` → `from_account`, exchange `q4_dt_detect_exchange`, routing keys `sg_detect_*`). Todos los escenarios aplican de forma simétrica. El riesgo principal es el mismo: crash durante el envío de EOFs a `sg_detect` genera EOFs duplicados en `destinations_eofs`.

---

## 8. Worker SgDetect

### Contexto

`SgDetect` es un worker multi-threaded (dos threads de consumo) que:

- Thread 1 consume del exchange de `og_detect` (origins), thread 2 del de `dt_detect` (destinations).
- Cuenta EOFs independientes: `origins_eofs[client_id]` y `destinations_eofs[client_id]`.
- Cuando ambos lados alcanzan su respectivo `NUM_*_WORKERS` → `_check_and_emit` (bajo lock).
- Archivos en disco: `/data/{client_id}/origins/{account}.csv` y `/data/{client_id}/destinations/{account}.csv`, escritos en modo `"w"` (sobreescritura).

Estado persistido en checkpoint:
- `last_origins_hash`, `last_destinations_hash`: hashes del último mensaje de datos de cada lado.
- `origins_eofs`, `destinations_eofs`: contadores de EOFs recibidos por cliente.

---

### Escenario 1: Caída durante procesamiento de datos (origins o destinations), antes de `_save_checkpoint`

- El archivo de cuenta se escribió con `"w"` → sobreescritura idempotente.
- Hash no se guardó.
- On restart: redelivery → hash no coincide → reprocesa → sobreescribe el mismo archivo con los mismos datos.

Resultado: correcto. Modo `"w"` hace que los datos sean idempotentes.

---

### Escenario 2: Caída después de `_save_checkpoint` en datos, antes de `ack`

Resultado: correcto. Hash coincide → descartado.

---

### Escenario 3: Caída durante procesamiento de EOF (origins o destinations), antes de `_save_checkpoint`

Cuándo ocurre: el worker incrementó el contador en memoria y opcionalmente llamó `_check_and_emit`, pero murió antes de `_save_checkpoint()`.

Estado al morir:
- Contador en checkpoint tiene el valor anterior al incremento.
- `_check_and_emit` puede haber corrido: emitió resultados, borró `client_dir`, eliminó contadores, envió EOF a `results_queue`.

Al reiniciar:
- Contador restaurado al valor anterior.
- RabbitMQ redelivera el EOF (el check de EOF va antes del hash → no se descarta).
- Contador se incrementa de nuevo → si llega a `NUM_*_WORKERS` con el otro lado también completo → `_check_and_emit` corre de nuevo.

Resultado: potencial duplicado de salida si `_check_and_emit` ya había corrido antes del crash.

---

### Escenario 4: Caída después de `_save_checkpoint` en EOF, antes de `ack`

Clave: `_check_and_emit` corre dentro del lock y elimina los contadores antes de que `_save_checkpoint` sea llamado (fuera del lock). Por eso el checkpoint refleja el estado post-eliminación de contadores.

- Si `_check_and_emit` sí corrió: el checkpoint guarda contadores vacíos. On restart: redelivery → contador va de 0 a 1 → no dispara `_check_and_emit` (1 < 3) → correcto. 
- Si `_check_and_emit` no corrió: el checkpoint guarda el count nuevo. On restart: redelivery → count sube uno más de lo esperado, pero el check es `>=` → no rompe la correctitud. 

Resultado: correcto en ambos casos.

---

### Escenario 5: Caída dentro de `_check_and_emit`, después de `_emit_results` pero antes del `output_queue.send` final

Cuándo ocurre: los resultados se enviaron, `client_dir` fue borrado, contadores eliminados, pero murió antes de `self.output_queue.send(serialize([client_id]))` (el EOF final a `results_queue`).

Al reiniciar:
- Checkpoint tiene contadores en el valor pre-`_check_and_emit`.
- RabbitMQ redelivera el EOF que disparó `_check_and_emit`.
- Count sube → `>= NUM_*_WORKERS` → `_check_and_emit` corre de nuevo.
- `_emit_results` intenta leer `client_dir` que ya no existe → no emite datos.
- `output_queue.send` envía el EOF final que faltaba.

Resultado: correcto. La segunda ejecución de `_check_and_emit` no re-emite datos (`client_dir` no existe) pero sí envía el EOF faltante.

---

## 9. Worker Converter

### Contexto

`Converter` es un worker single-threaded que:

- Consume batches de transacciones y convierte los montos a USD usando tasas de cambio de la API de frankfurter (con caché en memoria `_rate_lookup`).
- Usa el mismo protocolo de contador circulante que `filter` para coordinar EOFs entre instancias.
- No tiene estado acumulado complejo — no hay batches pendientes en memoria ni spill a disco.

Estado persistido en checkpoint:
- `last_msg_hash`: hash del último mensaje procesado.
- `eof_seen`: set de `client_id`s cuyo EOF ya procesó esta instancia.

Estado NO persistido:
- `_rate_lookup`: caché de tasas de cambio. Se pierde ante un reinicio pero no afecta la correctitud — solo implica re-consultar la API para tasas ya vistas.

---

### Escenario 1: Caída durante procesamiento de datos, antes de `_save_checkpoint`

Cuándo ocurre: el worker muere después de mandar las filas convertidas a output pero antes de guardar el checkpoint.

Estado al morir:
- Las filas convertidas ya se mandaron downstream.
- `last_msg_hash` en checkpoint tiene el hash del mensaje anterior.

Al reiniciar:
- RabbitMQ redelivera el mensaje.
- Hash no coincide → reprocesa → convierte y manda las mismas filas de nuevo.

Resultado: duplicado de filas en el siguiente worker.

La ventana sin cobertura es la caída entre `output_queue.send` y `_save_checkpoint`.

---

### Escenario 2: Caída después de `_save_checkpoint`, antes de `ack` (datos)

Al reiniciar: hash coincide con `last_msg_hash` → descartado.

Resultado: correcto.

---

### Escenario 3: Caída durante procesamiento de EOF, antes de `_save_checkpoint`

Mismo análisis que `filter` y `split_date`:

Resultado:
- **Caso cubierto (`counter=1`):** hash persistido en `_on_eof` → redelivery descartado. 
- **Caso no cubierto (`counter > 1` o `else`):** hash no persistido → redelivery entra como primera vez → EOF reencolado de nuevo — no hay duplicado de filas (no hay batches acumulados), pero el contador puede avanzar doble.

---

### Escenario 4: Caída después de `_save_checkpoint` en EOF, antes de `ack`

- Hash coincide → descartado (`counter=1`). 
- `eof_seen` tiene el `client_id` → entra al `else` → reencola sin decrementar (`counter > 1`). 

Resultado: correcto.
