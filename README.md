# Taller Data Engineer Junior Advanced (Apache Spark & PySpark)

Proyecto ETL local y de calidad de datos desarrollado con **Python 3.12**, **Java 17** y **PySpark 4.x** para simular las ventas de una tienda latinoamericana y resolver los desafíos del taller de ingeniería de datos.

## Estructura del Proyecto

```text
VianaDaniel/
├── datos/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── incremental/
│       ├── orders_incremental.jsonl
│       └── order_items_incremental.jsonl
├── src/
│   └── 01_profiling.py
├── output/          <-- Parquet limpios, agregaciones y carpeta quarantine/
├── README.md
├── .gitignore
└── .venv/
```

## Requisitos y Configuración del Entorno
- **Python**: 3.12 (gestionado en `.venv`)
- **Java**: OpenJDK 17 LTS
- **PySpark**: 4.2.0 / 4.3.0
- **Modo de Ejecución**: Local (`master("local[*]")`), sin dependencias cloud ni servicios externos.

## Ejecución del Pipeline (Fase 1 y 2: Ingesta y Perfilamiento)

Para activar el entorno virtual y ejecutar el perfilamiento:

```powershell
.\.venv\Scripts\activate
python src/01_profiling.py
```
