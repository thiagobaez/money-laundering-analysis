# Money Laundering Analysis

Alumnos: 
- Baez, Thiago Fernando - tbaez@fi.uba.ar - Padrón 110703
- Llanos Pontaut, Valentina - vllanos@fi.uba.ar - Padrón 104413

## Datasets

Bajar los datasets de [Kaggle - IBM Transactions for Anti-Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) y poner los archivos en el directorio `datasets/`.


## Expected Output

Bajar los [archivos de salida esperada](https://drive.google.com/drive/folders/1bOl9gDLcdXP1tUwwhLBHqjyLvgr9zjMQ?usp=sharing) y guardarlos dentro de la carpeta `expected` en el repositorio.

---

## Uso rápido

```
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

Lanza un asistente interactivo para elegir la query y configurar los workers. Cada parámetro muestra su valor por defecto entre corchetes, podes dar enter para aceptar ese valor o escribir otro y luego dar enter.

```
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

La opción `all` genera un compose que corre las 4 queries a la vez con un solo cliente. Los archivos se guardan en la raíz del proyecto.

Los scripts también se pueden correr directamente desde generate/:

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
