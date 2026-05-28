# Money Laundering Analysis

## Datasets

1. Descargar los datasets desde [Kaggle - IBM Transactions for Anti-Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
2. Colocar los archivos en el directorio `datasets/`

---

## Flujo de trabajo


```text
1. make generate      -> genera un docker-compose con los workers deseados
2. make switch-query  -> elige qué compose usar
3. make up            -> levanta el sistema y sigue los logs
4. make test          -> compara outputs para verificar
5. make down          -> baja todo y limpia
```

---

## Comandos Make

### `make generate` — generar un docker-compose

Lanza un asistente interactivo que pregunta qué query generar y los parámetros de cada worker. Para cada parámetro muestra el valor por defecto entre corchetes; presionar Enter lo acepta.

```text
$ make generate
=== Generador de docker-compose ===
Opciones: 1, 3, 4, 5, all
Que compose queres generar? [1/3/4/5/all]: 5

[ Query 5 — parametros ]
  input_file                   [HI-Medium_Trans.csv]:
  filter_fmt    (workers)      [7]:
  converter     (workers)      [3]:
  filter_amount (workers)      [2]:
  batch_size                   [10000]:
  output file                  [docker-compose-q5.yaml]:
```

La opción `all` genera un compose que corre las 4 queries simultáneamente con un solo cliente.

Los archivos generados se guardan en la raíz del proyecto (por ejemplo `docker-compose-q5.yaml`).

Los scripts individuales también se pueden correr directamente desde la carpeta `generate/`:

| Query  | Script                               |
| ------ | ------------------------------------ |
| Q1     | `generate/generate_compose_q1.py`    |
| Q3     | `generate/generate_compose_q3.py`    |
| Q4     | `generate/generate_compose_q4.py`    |
| Q5     | `generate/generate_compose_q5.py`    |
| Todas  | `generate/generate_compose_all.py`   |

---

### `make switch-query` — elegir qué compose ejecutar

Lista todos los archivos `.yaml` de la raíz del proyecto ordenados alfabéticamente y pide elegir uno por número. El seleccionado queda guardado en `.compose` y es el que usarán `make up`, `make down` y `make logs`.

```text
$ make switch-query
  1) docker-compose-q1.yaml
  2) docker-compose-q3.yaml
  3) docker-compose-q4.yaml
  4) docker-compose-q5.yaml
Seleccionar compose [1-4]: 4
Usando: docker-compose-q4.yaml
```

Si nunca se ejecutó `make switch-query`, el sistema busca `docker-compose.yaml` por defecto (que puede no existir).

---

### `make up` — levantar el sistema

Construye las imágenes, levanta todos los contenedores en segundo plano y sigue los logs en la terminal.

```bash
make up
```

Cuando el cliente termina de procesar el dataset y recibe todos los resultados, los escribe en `output/client0/query{N}/`:


---

### `make down` — bajar y limpiar

Detiene los contenedores, elimina volúmenes e imágenes, y borra el directorio `output/` y los archivos temporales de Q3.

```bash
make down
```

> **Nota:** usa `sudo` internamente para borrar `output/` y `src/business/query3/spill_to_disk/`.

---

### `make logs` — ver logs

Muestra los logs del compose activo sin seguirlos en tiempo real.

```bash
make logs
```

---

### `make lint` / `make test-unit` / `make test-e2e`

```bash
make lint        # ruff check src/
make test-unit   # pytest tests/unit/
make test-e2e    # pytest tests/e2e/
make all         # lint + test-unit
```

---

## Comparar resultados

El script `compare_output.py` compara la salida obtenida en `output/client0/` contra los resultados esperados en `expected/`.

```bash
python3 compare_output.py
```

Pregunta interactivamente por qué query comparar (`1`, `3`, `4`, `5` o `all`). Compara tanto el `count.csv` como el `tx.csv` (sin importar el orden de las filas).


---

## Queries disponibles

**Q1** — Transacciones en USD mayores a $50  
Workers: `filter_usd`, `filter_amount`

**Q3** — Promedio de transacciones por formato de pago en dos períodos  
Workers: `filter_usd`, `split_date`, `avg`, `avg_joiner`

**Q4** — Detección de cuentas intermediarias entre origen y destino  
Workers: `filter_usd`, `filter_date`, `split`, `og_detect`, `dt_detect`, `sg_detect`

**Q5** — Transacciones Wire/ACH en septiembre 2022 con monto < $1 USD  
Workers: `filter_fmt`, `converter`, `filter_amount`
