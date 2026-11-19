# Fase 1 y Fase 2: Ingesta Cruda y Perfilamiento de Calidad de Datos

## 1. Objetivo de la Fase
Construir un módulo de observabilidad (`src/01_profiling.py`) para cargar los 4 datasets históricos en formato CSV (`customers`, `products`, `orders` y `order_items`) aplicando lectura controlada (`inferSchema=False`, `nullValue="NULL"`) y tipado explícito (`F.try_cast`), con el fin de diagnosticar anomalías de unicidad, integridad referencial, nulos y dominios de negocio antes de transformar o calcular métricas financieras.

---

## 2. Arquitectura Técnica Implementada

1. **Extracción hacia Formatos Nativos de Spark:**
   Dado que Apache Spark no lee archivos `.xlsx` de forma nativa[cite: 1], se extrajeron las 6 hojas del libro `taller_data_engineer.xlsx` hacia la carpeta `datos/` en los formatos estándar del pipeline[cite: 1, 4]:
   - **Históricos (`.csv`):** `customers.csv`, `products.csv`, `orders.csv` y `order_items.csv`[cite: 4].
   - **Incrementales (`.jsonl`):** `datos/incremental/orders_incremental.jsonl` y `datos/incremental/order_items_incremental.jsonl`[cite: 4].

2. **Ingesta Cruda Controlada (`inferSchema=False`):**
   Todos los CSV se leyeron inicialmente como texto plano (`StringType`) usando `.option("header", True)` y `.option("inferSchema", False)`[cite: 3, 4], evitando que Spark infiera tipos erróneos o colapse ante registros corruptos.

3. **Tipado Explícito Tolerante a Fallos (`try_cast`):**
   Se castearon explícitamente fechas (`DateType`), marcas de tiempo (`TimestampType`), precios/descuentos (`DoubleType`), cantidades (`IntegerType`) y estados activos (`BooleanType`). Al detectar celdas con la cadena literal `"NULL"` (que disparaban `SparkDateTimeException` bajo el modo ANSI estricto de Spark 4), se configuró `.option("nullValue", "NULL")` y `F.try_cast()` para convertir cadenas mal formadas en `null` reales sin detener la ejecución.

4. **Auditorías de Calidad de Datos (4 Dimensiones):**
   - **Unicidad de PK:** Contraste entre total de filas y claves distintas (`distinct().count()`).
   - **Completitud:** Conteo de nulos y campos vacíos columna por columna.
   - **Dominios y Rangos:** Validación de precios, cantidades, descuentos y estados permitidos.
   - **Integridad Referencial (`left_anti` join):** Cruces anti-join para identificar claves foráneas huérfanas sin pareja en sus tablas maestras.

---

## 3. Volumetría Inicial

| Tabla | Filas Totales | Clave Primaria (PK) | PKs Únicas | Duplicados |
| :--- | :---: | :--- | :---: | :---: |
| `customers.csv` | 1,001 | `customer_id` | 1,000 | 1 (`C0020`) |
| `products.csv` | 100 | `product_id` | 100 | 0 |
| `orders.csv` | 20,001 | `order_id` | 20,000 | 1 (`O00012`) |
| `order_items.csv` | 40,026 | `line_id` | 40,026 | 0 |

---

## 4. Anomalías Detectadas y Estrategia de Tratamiento

1. **Duplicados en Claves Primarias:**
   - `customers`: `C0020` repetido 2 veces → *Decisión:* Deduplicar conservando el registro más completo.
   - `orders`: `O00012` repetido 2 veces → *Decisión:* Deduplicar conservando el registro más reciente según `updated_at`.
2. **Nulos Literales y Mal Formados:**
   - Cadenas literales `"NULL"` en fechas (`signup_date`) y atributos (`email` en `C0013`, `customer_id` en `O00011`) → *Decisión:* Tolerar con `try_cast`, imputar `email` no crítico con valor por defecto y enviar pedidos sin cliente a cuarentena.
3. **Error Tipográfico en Dominio (`status`):**
   - Valor `'deliverd'` en `orders` → *Decisión:* Estandarizar a `'delivered'`.
4. **Valores Fuera de Rango:**
   - `products`: `P088` con `unit_price = -12.50` y `P089` con precio nulo → *Decisión:* Enviar a cuarentena (`output/quarantine/products/`).
   - `order_items`: `L0000001` (`quantity = 0`), `L0000002` (`quantity = -2`) y `L0000005` (`discount_pct = 1.20`) → *Decisión:* Enviar a cuarentena (`output/quarantine/order_items/`).
5. **Integridad Referencial (Registros Huérfanos):**
   - `orders`: `O00007` referencia `customer_id = C9999` inexistente → *Decisión:* Enviar a cuarentena (`output/quarantine/orders/`).
   - `order_items`: `L0000003` referencia `O99999` y `L0000004` referencia `P999` → *Decisión:* Enviar a cuarentena (`output/quarantine/order_items/`).

---

## 5. Resumen Ejecutivo (Sustentación)
En la Fase 1 y 2 no se transformaron datos ni se calcularon métricas de venta todavía; **se construyó un escudo de observabilidad**. Se garantizó el tipado estricto de los esquemas y se dejó evidencia trazable de cada registro defectuoso para asegurar que, en la siguiente fase, ningún dato corrupto contamine los reportes financieros ni sea descartado silenciosamente sin pasar por cuarentena[cite: 5].