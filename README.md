# Money Laundering Analysis

## Uso

### Datasets

1. Descargar los datasets desde [Kaggle - IBM Transactions for Anti-Money Laundering](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
2. Colocar los archivos CSV descargados en el directorio `datasets/`

### Ejecutar el sistema

```bash
make switch   # Seleccionar qué query ejecutar (menú interactivo)
make up       # Levantar los servicios
make down     # Bajar los servicios
make logs     # Ver logs de los servicios
```

El comando `make switch` muestra un menú para elegir la query a ejecutar. Luego usar `make up` para iniciar el sistema con la query seleccionada.

### Notebook de referencia

Desde el directorio `reference/`:

```bash
jupyter nbconvert --to notebook --execute money-laundering-analysis.ipynb --output executed.ipynb
```

Si se usa un dataset distinto al provisto, actualizar en `money-laundering-analysis.ipynb` las rutas a los archivos CSV de transacciones y cuentas.