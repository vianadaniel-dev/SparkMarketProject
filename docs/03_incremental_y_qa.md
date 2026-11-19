# Fase 5 y Fase 6: Ingesta Incremental Idempotente y Suite de Calidad (QA)

## 1. Objetivos del Pipeline Incremental
1. **Fase 5 (Ingesta y Upsert Incremental Idempotente):**
   - Procesar lotes incrementales en formato JSON Lines (`orders_incremental.jsonl` y `order_items_incremental.jsonl`).
   - Aplicar el mismo rigor de tipado explícito (`try_cast`), corrección de dominios y segregación a cuarentena sin descartes silenciosos.
   - Diseñar un mecanismo de **Upsert Idempotente** combinando datos históricos con el lote incremental, garantizando que actualizaciones tardías (*late-arriving updates*) sobrescriban versiones previas según `updated_at` más reciente.
   - Asegurar que la reejecución múltiple del pipeline sobre el mismo lote no duplique registros ni altere los importes acumulados.

2. **Fase 6 (Suite de Calidad Automatizada - QA):**
   - Integrar 3 controles de calidad automatizados mediante sentencias `assert` directas en el flujo de ejecución.
   - Verificar reconciliación financiera al centavo, unicidad estricta de claves y prueba de fuego de idempotencia en doble pasada.

---

## 2. Ingesta y Segregación de Calidad del Lote Incremental

Los archivos incrementales fueron leídos y contrastados contra las dimensiones limpias:

| Archivo Incremental | Registros Totales | Registros Limpios | Registros en Cuarentena | Causa de Segregación |
| :--- | :---: | :---: | :---: | :--- |
| `orders_incremental.jsonl` | **101** | **101** | 0 | 100 pedidos nuevos (`O20001` - `O20100`) + 1 actualización tardía (`O00005`). Clientes válidos y fechas íntegras. |
| `order_items_incremental.jsonl` | **200** | **196** | **4** | 4 ítems segregados a `output/quarantine/order_items/` con motivo `product_id not found in clean products` (asociados a productos rechazados en Fase 2). |

---

## 3. Estrategia de Upsert Idempotente y Actualización Tardía

### A. Pedidos (`orders`)
* Se combinaron las órdenes históricas limpias (19,997) y las órdenes incrementales (101) mediante `unionByName`.
* Se aplicó una ventana de particionamiento sobre `order_id` ordenada descendentemente por `updated_at`:
  ```python
  w_orders_upsert = Window.partitionBy("order_id").orderBy(
      F.col("updated_at").desc(), F.col("_source_priority").desc()
  )
  ```
* **Caso de Estudio (Actualización Tardía `O00005`):**
  * Versión histórica: `updated_at = 2026-02-22T15:20:00Z` (canal: `store`).
  * Versión incremental: `updated_at = 2026-03-05T12:00:00Z` (canal: `store`).
  * **Resultado:** La versión más reciente prevaleció limpiamente, consolidando exactamente **20,097 pedidos únicos** (19,997 + 100 nuevos).

### B. Líneas de Detalle (`order_items`)
* Se unieron los ítems históricos limpios (39,190) y los ítems incrementales limpios (196).
* Se deduplicó por `line_id` priorizando el lote incremental en caso de colisión:
  ```python
  w_items_upsert = Window.partitionBy("line_id").orderBy(F.col("_source_priority").desc())
  ```
* **Resultado:** Consolidación de exactamente **39,386 líneas de pedido válidas** (39,190 + 196).

---

## 4. Evidencia de los 3 Controles de Calidad Automatizados (QA Suite)

La suite de validación integrada en `src/03_incremental_and_qa.py` ejecutó 3 pruebas obligatorias con `assert`:

### QA Check 1: Reconciliación Financiera Detalle vs. Agregado
Verifica que la agregación multidimensional no altere ni pierda importe neto respecto al nivel línea:
$$\text{Diferencia} = \left| \sum \text{net\_amount}_{\text{sales\_detail\_clean}} - \sum \text{total\_net\_amount}_{\text{sales\_monthly\_summary}} \right|$$

* **Suma Detalle (`sales_detail_clean`):** **\$95,703,942.43**
* **Suma Resumen (`sales_monthly_summary`):** **\$95,703,942.43**
* **Diferencia Absoluta:** **\$0.00** (`diff < 0.01`)
* **Estado:** `[+] QA CHECK 1 APROBADO`.

### QA Check 2: Unicidad e Integridad de Atributos Críticos
Audita que tras el merge y joins no existan colisiones de claves primarias ni valores nulos en variables esenciales:
* **Duplicados en `order_id` (post-merge):** **0** (`assert dup_orders == 0`)
* **Duplicados en `line_id` (post-merge):** **0** (`assert dup_items == 0`)
* **Nulos en atributos clave de `sales_detail_clean`:**
  * `line_id`: 0 nulos
  * `order_id`: 0 nulos
  * `customer_id`: 0 nulos
  * `product_id`: 0 nulos
  * `net_amount`: 0 nulos
* **Estado:** `[+] QA CHECK 2 APROBADO`.

### QA Check 3: Prueba Real de Idempotencia (Doble Pasada Incremental)
Se sometió el dataset final consolidado a una segunda ejecución del proceso incremental aplicando exactamente los mismos datos sobre la salida generada:

| Métrica Auditada | Pasada 1 (Salida Inicial) | Pasada 2 (Reprocesamiento) | Discrepancia | Evaluación |
| :--- | :---: | :---: | :---: | :---: |
| **Total Líneas (`sales_detail_clean`)** | 39,386 | 39,386 | **0** | `assert pass1_lines == pass2_lines` |
| **Importe Neto Consolidado** | \$95,703,942.43 | \$95,703,942.43 | **\$0.00** | `assert amount_diff == 0.0` |

* **Estado:** `[+] QA CHECK 3 APROBADO (Idempotencia Absoluta Garantizada)`.

---

## 5. Salidas Parquet Definitivas y Volumetría Final

```text
output/
├── quarantine/
│   ├── products/          (2 registros Parquet - unit_price inválido o nulo)
│   ├── orders/            (3 registros Parquet - customer_id huérfano/nulo)
│   └── order_items/       (840 registros Parquet = 836 históricos + 4 incrementales)
├── sales_detail_clean/    (39,386 registros Parquet enriquecidos a nivel línea)
└── sales_monthly_summary/ (208 grupos analíticos Parquet por mes, país, canal y categoría)
```
