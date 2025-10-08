# Inventario Contifico

Herramientas básicas para sincronizar el catálogo de Contifico con un almacén local.

## Requisitos iniciales

1. Crear un entorno virtual:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```
3. Copiar el archivo de variables de entorno y completarlo:
   ```bash
   cp .env.example .env
   ```

## Variables de entorno

| Variable | Descripción |
| --- | --- |
| `CONTIFICO_API_KEY` | API Key provista por Contífico. |
| `CONTIFICO_API_TOKEN` | API Token asociado a la clave anterior. |
| `CONTIFICO_API_BASE_URL` | (Opcional) URL base de la API, útil para entornos de prueba. |
| `INVENTORY_DB_PATH` | (Opcional) Ruta al archivo SQLite. Por defecto `data/inventory.db`. |

Las variables se cargan automáticamente mediante [`python-dotenv`](https://github.com/theskumar/python-dotenv).

## Sincronización de datos

El script `src/ingestion/sync_inventory.py` consulta los recursos principales de Contifico
(productos, compras, ventas y bodegas) empleando la autenticación con API Key y Token, y
almacena la información en SQLite. Para ejecutar una sincronización completa:

```bash
python -m src.ingestion.sync_inventory
```

### Parámetros opcionales

- `--since`: fecha/hora en formato ISO8601 para forzar el punto inicial de importación (se envía a la API como `fecha_modificacion__gte`).
- `--batch-size`: número de registros que se escriben por transacción (por defecto 100).

### Esquema de la base de datos

El repositorio crea automáticamente un archivo SQLite con las tablas `products`, `purchases`,
`sales`, `warehouses` y la tabla auxiliar `sync_state` para almacenar la última ejecución por
endpoint. Cada registro incluye la versión completa del JSON devuelto por la API y marcas de
actualización (`updated_at`) y de captura (`fetched_at`).

## Estructura del proyecto

```
├── src/
│   ├── contifico_client.py    # Cliente HTTP para la API de Contifico
│   ├── persistence.py         # Acceso a la base de datos SQLite
│   ├── ingestion/
│   │   └── sync_inventory.py  # Script de sincronización incremental
│   └── web/
│       ├── app.py             # Aplicación FastAPI para visualizar el inventario
│       ├── templates/         # Vistas Jinja2 para el panel web
│       └── static/            # Recursos estáticos (CSS, imágenes, etc.)
├── requirements.txt
├── .env.example
└── README.md
```

## Plataforma web

La carpeta `src/web` contiene una aplicación [FastAPI](https://fastapi.tiangolo.com/) con vistas
en Jinja2 que permite consultar el estado del inventario sincronizado y servirá de base para los
análisis que se añadirán más adelante.

Para ejecutar la aplicación localmente:

```bash
uvicorn src.web.app:app --reload
```

Luego abre <http://127.0.0.1:8000> en tu navegador. La página principal muestra un resumen de los
recursos sincronizados (número de registros y fechas de actualización) y un roadmap con los
próximos análisis a construir.

## Próximos pasos sugeridos

- Añadir pruebas automáticas para la capa de persistencia y la API.
- Incorporar visualizaciones de KPIs y comparativas históricas en el panel web.
- Extender el cliente con endpoints adicionales según las necesidades del negocio.
