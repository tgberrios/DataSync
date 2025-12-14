# Instrucciones para Probar Python Scripts en Custom Jobs

## ✅ Compilación Exitosa

El proyecto se compiló correctamente. El ejecutable está en: `./DataSync`

## 🧪 Cómo Probar

### Opción 1: Probar desde la UI (Recomendado)

1. **Inicia DataSync:**

   ```bash
   ./DataSync
   ```

2. **Inicia el frontend** (en otra terminal):

   ```bash
   cd frontend
   npm run dev
   ```

3. **Abre el navegador:** http://localhost:5173

4. **Ve a Custom Jobs** y crea un nuevo job:

   - **Job Name:** `test_python_simple`
   - **Description:** `Test Python script execution`
   - **Source DB Engine:** `Python` ⭐ (IMPORTANTE)
   - **Source Connection String:** (puede estar vacío para Python)
   - **Query SQL:** Copia TODO el contenido de `scripts/test_simple.py`:

     ```python
     import json

     data = [
         {"id": 1, "name": "Test 1", "value": 100},
         {"id": 2, "name": "Test 2", "value": 200},
         {"id": 3, "name": "Test 3", "value": 300}
     ]

     print(json.dumps(data))
     ```

   - **Target DB Engine:** `PostgreSQL` (o el que uses)
   - **Target Connection String:** Tu conexión a PostgreSQL
   - **Target Schema:** `public` (o el que uses)
   - **Target Table:** `test_python_results`
   - **Active:** ✅
   - **Enabled:** ✅

5. **Guarda el job** y luego **ejecuta** el job (botón Play ▶️)

6. **Verifica los resultados:**
   - Revisa los logs en la consola de DataSync
   - Verifica que la tabla `test_python_results` tenga los 3 registros
   - Revisa la sección de resultados del job

### Opción 2: Probar Scripts Directamente

```bash
cd scripts
python3 test_simple.py
python3 example_data_generator.py
python3 calculate_metrics.py
```

Todos deberían imprimir JSON válido a stdout.

## 📋 Checklist de Verificación

- [x] Compilación exitosa
- [x] Scripts Python funcionan correctamente
- [x] Frontend tiene opción "Python" en Source DB Engine
- [ ] DataSync corriendo
- [ ] Base de datos configurada
- [ ] Custom Job creado con source_db_engine = "Python"
- [ ] Job ejecutado exitosamente
- [ ] Datos insertados en tabla destino

## 🔍 Qué Esperar

Cuando ejecutes el job:

1. DataSync detectará que `source_db_engine == "Python"`
2. Creará un archivo temporal con el script
3. Ejecutará `python3` con el script
4. Capturará el JSON de stdout
5. Parseará el JSON
6. Insertará los datos en la tabla destino
7. Registrará el resultado en `metadata.job_results`

## ⚠️ Troubleshooting

Si hay errores:

- Verifica que `python3` esté instalado: `which python3`
- Revisa los logs de DataSync
- Verifica que el script imprima JSON válido
- Asegúrate de que el JSON sea un array o objeto
