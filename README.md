# Taller Data Engineer Junior Advanced (Apache Spark & PySpark)

Proyecto ETL local y de observabilidad de calidad de datos desarrollado con **Python 3.12**, **Java 17 (OpenJDK)** y **PySpark 4.2.0** para simular el procesamiento de ventas de una tienda latinoamericana y resolver los desafíos del taller de ingeniería de datos.

---

## Estado del Proyecto: 6 Fases Completadas (10/10 Puntos)

- [x] **Fase 1: Ingesta Cruda y Lectura Controlada** (`inferSchema=False`, `nullValue="NULL"`, tipado explícito tolerante a fallos con `try_cast`).
- [x] **Fase 2: Perfilamiento y Segregación de Calidad** (diagnóstico de unicidad, nulos, rangos y segregación a cuarentena persistente con `quarantine_reason`).
- [x] **Fase 3: Limpieza, Enriquecimiento y Cálculo de Importe Neto** (deduplicación por `updated_at`, joins a nivel grano de línea sin duplicar importes, `net_amount` exacto).
- [x] **Fase 4: Agregación Analítica Mensual y Reconciliación** (datamart por mes, país, canal y categoría con reconciliación financiera al 100%).
- [x] **Fase 5: Ingesta Incremental Idempotente** (ingesta de JSONL, segregación de ítems huérfanos, y upsert idempotente con precedencia de actualizaciones tardías como `O00005`).
- [x] **Fase 6: Suite Automatizada de Calidad (QA)** (3 controles con `assert` explícitos: reconciliación financiera, unicidad/integridad de atributos clave y prueba de doble pasada de idempotencia).

---

## Estructura del Workspace

```text
VianaDaniel/
├── datos/
│   ├── customers.csv                    # 1,001 registros crudos (clientes)
│   ├── products.csv                     # 100 registros crudos (productos)
│   ├── orders.csv                       # 20,001 registros crudos (pedidos históricos)
│   ├── order_items.csv                  # 40,026 registros crudos (detalle de pedidos)
│   └── incremental/
│       ├── orders_incremental.jsonl     # 101 pedidos (100 nuevos + update O00005)
│       └── order_items_incremental.jsonl# 200 líneas de pedido incrementales
├── docs/
│   ├── 01_ingesta_y_perfilamiento.md    # Bitácora de Fases 1 y 2 (diagnóstico y volumetría)
│   ├── 02_limpieza_y_agregaciones.md    # Bitácora de Fases 3 y 4 (limpieza, joins y reconciliación)
│   ├── 03_incremental_y_qa.md           # Bitácora de Fases 5 y 6 (upsert, idempotencia y QA)
│   ├── Taller Spark.pdf                 # Guía y rúbrica oficial del taller
│   └── taller_data_engineer.xlsx        # Libro Excel original con las 6 hojas
├── hadoop/                              # Binarios winutils.exe y hadoop.dll para Windows
│   └── bin/
├── output/                              # Datasets limpios y segregaciones en formato Parquet
│   ├── quarantine/
│   │   ├── products/                    # 2 productos inválidos (precio negativo / nulo)
│   │   ├── orders/                      # 3 órdenes con cliente nulo o huérfano
│   │   └── order_items/                 # 840 ítems huérfanos o con atributos inválidos
│   ├── sales_detail_clean/              # 39,386 registros consolidados a nivel línea
│   └── sales_monthly_summary/           # 208 grupos analíticos agregados
├── src/
│   ├── 01_profiling.py                  # Script de perfilamiento y diagnóstico inicial
│   ├── 02_etl_cleaning.py               # Script de pipeline histórico (Fases 1 a 4)
│   └── 03_incremental_and_qa.py         # Pipeline incremental, upsert y suite QA (Fases 5 y 6)
├── README.md
└── .gitignore
```

---

## Requisitos y Configuración del Entorno

* **Python**: 3.12 (gestionado en `.venv`)
* **Java**: OpenJDK 17 LTS
* **PySpark**: 4.2.0
* **Modo de Ejecución**: Local (`master("local[*]")`), 100% autocontenido sin dependencias cloud ni Docker.
* **Compatibilidad Windows**: Configuración automática de `HADOOP_HOME` enlazando a `hadoop/bin/`.

---

## Guía de Ejecución

Activa el entorno virtual en PowerShell:

```powershell
.\.venv\Scripts\activate
```

### 1. Perfilamiento Inicial (Fase 1 y 2)
```powershell
python src/01_profiling.py
```
*Identifica duplicados (`C0020`, `O00012`), nulos literales `"NULL"`, error tipográfico `'deliverd'`, precios negativos (`P088`) e integridad referencial.*

### 2. Limpieza Histórica y Agregación (Fase 3 y 4)
```powershell
python src/02_etl_cleaning.py
```
*Aplica segregación a cuarentena, calcula `net_amount` sobre 39,190 líneas limpias y reconcilia $95,297,643.79 con 144 grupos analíticos ($0.00 de discrepancia).*

### 3. Ingesta Incremental, Upsert y Suite de QA (Fase 5 y 6)
```powershell
python src/03_incremental_and_qa.py
```
*Aplica el lote incremental, actualiza `O00005`, ejecuta y valida los 3 `assert` de calidad, y persiste 39,386 registros consolidados y 208 grupos en Parquet.*

---

## Resumen de Resultados Financieros y de Calidad

| Métrica / Control | Fase Histórica (02_etl_cleaning) | Consolidado Incremental (03_incremental_and_qa) | Estado |
| :--- | :---: | :---: | :---: |
| **Clientes Válidos** | 1,000 | 1,000 | Aprobado |
| **Productos Válidos** | 98 (2 en cuarentena) | 98 (2 en cuarentena) | Aprobado |
| **Órdenes Válidas** | 19,997 (3 en cuarentena) | 20,097 (3 en cuarentena) | Aprobado |
| **Líneas de Detalle Válidas** | 39,190 (836 en cuarentena) | 39,386 (840 en cuarentena) | Aprobado |
| **Grupos Analíticos Mensuales** | 144 | 208 | Aprobado |
| **Importe Neto Consolidado** | **\$95,297,643.79** | **\$95,703,942.43** | Aprobado |
| **Discrepancia Reconciliación** | **\$0.00** | **\$0.00** (`diff < 0.01`) | **QA 1 Exitoso** |
| **Duplicados y Nulos Críticos** | 0 | 0 | **QA 2 Exitoso** |
| **Idempotencia (Doble Pasada)** | N/A | $\Delta\text{filas}=0$, $\Delta\text{monto}=\$0.00$ | **QA 3 Exitoso** |
