# Money Laundering Analysis

## Uso

### Datasets

1. Descargar los datasets desde [Kaggle - IBM Transactions for Anti-Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
2. Colocar los archivos en el directorio `datasets/`

### Ejecutar el sistema

```bash
make switch   # Seleccionar qué query ejecutar (menú interactivo)
make up       # Levantar los servicios
make down     # Bajar los servicios
make logs     # Ver logs de los servicios
```

El comando `make switch` muestra un menú para elegir la query a ejecutar. Luego usar `make up` para iniciar el sistema con la query seleccionada.

### Query 5 — configuración de workers

El docker compose de Q5 se genera con el script `generate_compose_q5.py`:

```bash
python3 generate_compose.py \
  --filter-fmt 2 \
  --converter 2 \
  --filter-amount 2 \
  --input-file HI-Small_Trans.csv.gz \
  --send-rate-limit 0.001 \
  --output docker-compose-q5.yaml
```

Parámetros disponibles:

- `--filter-fmt`: cantidad de workers `filter_q5_fmt` (default: 2)
- `--converter`: cantidad de workers `converter` (default: 2)
- `--filter-amount`: cantidad de workers `filter_q5_amount` (default: 2)
- `--input-file`: archivo de dataset en `datasets/` (default: `HI-Small_Trans.csv.gz`)
- `--send-rate-limit`: delay en segundos entre mensajes del gateway (default: 0.001)
- `--output`: nombre del archivo generado (default: `docker-compose-q5.yaml`)

Luego seleccionar Q5 con `make switch` y levantar con `make up`.