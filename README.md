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
| `SYNC_BATCH_SIZE` | (Opcional) Tamaño de lote usado para escritura en base de datos. |
| `CONTIFICO_PAGE_SIZE` | (Opcional) Registros solicitados por página a la API (por defecto 200). |
| `LOG_LEVEL` | (Opcional) Nivel de logging (`INFO`, `DEBUG`, etc.) para ver el detalle de las operaciones. |
| `LOG_FILE` | (Opcional) Ruta de archivo donde persistir los logs además de la consola. |

Las variables se cargan automáticamente mediante [`python-dotenv`](https://github.com/theskumar/python-dotenv).

## Sincronización de datos

La plataforma web expone un botón de **"Sincronizar ahora"** que lanza, en segundo plano, la
descarga de todos los catálogos disponibles en la API pública de Contífico y guarda los resultados
en la base SQLite configurada. Se incluyen los módulos de inventario (categorías, marcas,
variantes, productos, bodegas, guías de remisión y movimientos), los documentos del registro
(`GET /registro/documento/` para ventas y compras), el catálogo general de documentos (`GET
/documento/`), las transacciones (`GET /registro/transaccion/`), las personas (`GET /persona/`), los
componentes contables (`GET /contabilidad/centro-costo/`, `GET /contabilidad/cuenta-contable/`, `GET
/contabilidad/asiento/`) y los servicios bancarios (`GET /banco/cuenta/`, `GET /banco/movimiento/`).
Opcionalmente puedes indicar un punto de partida (`since`) usando el selector de fecha/hora para
restringir la importación a cambios recientes.

Para grandes volúmenes de información la sincronización se realiza en lotes: el cliente solicita
páginas al API (`CONTIFICO_PAGE_SIZE`) y la capa de persistencia agrupa los registros recibidos
(`SYNC_BATCH_SIZE`) antes de confirmarlos en disco. Así evitamos saturar memoria al descargar todos
los movimientos y documentos históricos.

Desde el formulario web puedes elegir **qué módulos sincronizar** (deja las casillas vacías para
traer todo) y activar un modo de **descarga completa** que ignora el historial guardado para volver a
pedir cada documento.

El panel consume internamente el endpoint `POST /api/sync`, que queda disponible si deseas
integrarlo con otras herramientas (por ejemplo, programar sincronizaciones desde un cron externo):

```bash
curl -X POST "http://localhost:8000/api/sync?since=2024-01-01T00:00&resources=products&resources=inventory_movements"
```

También puedes forzar una recarga total desde la API agregando `full_refresh=true` en la URL.

Si necesitas ejecutar la sincronización fuera del entorno web, el módulo
`src/ingestion/sync_inventory.py` expone la misma lógica a través de la función
`synchronise_inventory` y mantiene la interfaz de línea de comandos como alternativa.
Los argumentos opcionales permiten seleccionar módulos (`--resources products sales`), forzar un
recorrido completo (`--full-refresh`) o ajustar el paginado remoto (`--page-size 500`).

### Registro de actividad y diagnósticos

Para auditar las peticiones hechas a Contífico y depurar errores, activa el modo detallado en tu
archivo `.env`:

```ini
LOG_LEVEL=DEBUG
LOG_FILE=logs/contifico.log
```

Con estos ajustes el sistema registrará cada request y response (incluyendo parámetros y cuerpos
truncados) tanto en consola como en el archivo indicado. El directorio del archivo se crea de forma
automática. Revisa el log cuando se produzcan errores de sincronización para identificar qué payload
generó la respuesta de error de la API.

### Esquema de la base de datos

El repositorio crea automáticamente un archivo SQLite con una tabla por endpoint sincronizado:
`categories`, `brands`, `variants`, `products`, `warehouses`, `inventory_movements`,
`remission_guides`, `purchases`, `sales`, `documents`, `registry_transactions`, `persons`,
`cost_centers`, `chart_of_accounts`, `journal_entries`, `bank_accounts`, `bank_movements` y la
tabla auxiliar `sync_state` para almacenar la última ejecución por recurso. Cada registro incluye la
versión completa del JSON devuelto por la API, marcas de actualización (`updated_at`,
`fecha_modificacion`, `fecha`, etc.) y de captura (`fetched_at`).

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
recursos sincronizados (número de registros y fechas de actualización), un formulario para lanzar
sincronizaciones y un roadmap con los próximos análisis a construir.

## Despliegue recomendado en una VPS

1. **Preparar el servidor**
   - Instala Python 3.11 o superior y herramientas básicas: `sudo apt update && sudo apt install python3-venv git`.
   - Crea un usuario dedicado (opcional) y clona el repositorio en `/opt/inventario-contifico`.

2. **Configurar el entorno de ejecución**
   ```bash
   cd /opt/inventario-contifico
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   cp .env.example .env  # completa las credenciales de Contífico
   mkdir -p data && touch data/inventory.db
   ```

3. **Servicio con Uvicorn + Systemd** (archivo `/etc/systemd/system/inventario.service`):

   ```ini
   [Unit]
   Description=Inventario Contifico
   After=network.target

   [Service]
   User=www-data
   WorkingDirectory=/opt/inventario-contifico
   Environment="PATH=/opt/inventario-contifico/.venv/bin"
   Environment="LOG_LEVEL=INFO"
   Environment="LOG_FILE=/opt/inventario-contifico/logs/contifico.log"
   ExecStart=/opt/inventario-contifico/.venv/bin/uvicorn src.web.app:app --host 0.0.0.0 --port 8000
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   ```

   Crea el directorio de logs (`sudo mkdir -p /opt/inventario-contifico/logs && sudo chown www-data:www-data /opt/inventario-contifico/logs`).
   Aplica los cambios: `sudo systemctl daemon-reload && sudo systemctl enable --now inventario`.

4. **Exponer la aplicación**
   - Para HTTPS y dominio propio, configura un proxy inverso (Nginx o Caddy) que apunte a
     `http://127.0.0.1:8000` y gestione certificados con Let's Encrypt.
   - Si deseas orquestar sincronizaciones programadas, puedes usar `cron` para invocar el endpoint
     `POST /api/sync` mediante `curl` o `systemd timers`.

## Próximos pasos sugeridos

- Añadir pruebas automáticas para la capa de persistencia y la API.
- Incorporar visualizaciones de KPIs y comparativas históricas en el panel web.
- Extender el cliente con endpoints adicionales según las necesidades del negocio.
