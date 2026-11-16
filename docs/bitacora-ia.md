# Bitácora de uso de IA

Registro de las consultas hechas a herramientas de IA durante el proyecto. En el código, los fragmentos asistidos están marcados con el comentario `# Asistido por IA:`.

| Fecha | Integrante | Herramienta | Consulta | Resultado / uso | Archivos |
|-------|------------|-------------|----------|-----------------|----------|
| 2026-11-15 | Matias Sinchi | Claude Code | Revisar los commits nuevos y resumir los avances | Resumen de los cambios y lista de problemas de diseño (consumidores sin control de duplicados, `version` sin uso, estado sin paso intermedio) | — |
| 2026-11-15 | Matias Sinchi | Claude Code | Instalar `confluent-kafka` en Python 3.15 y probar el flujo completo | Se instaló `librdkafka` con brew y se probó el flujo de punta a punta. Se detectó que `cocina.py` y `caja.py` se caían si el topic aún no existía | — |
| 2026-11-15 | Matias Sinchi | Claude Code | Corregir la caída por topic inexistente y completar el README | Ignorar `UNKNOWN_TOPIC_OR_PART` en los consumidores; README con instalación y ejecución | `cocina.py`, `caja.py`, `readme.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | Crear `ventas.py` con grupo `servicio-ventas`, conteo de pedidos y unidades, control de `eventId`, commit tras procesar y cierre con Control + C | Servicio de ventas implementado | `ventas.py` |
| 2026-11-16 | Matias Sinchi | Claude Code | Ejecutar y documentar la prueba de recuperación (detener Cocina, crear pedido, reiniciar Cocina) y una prueba de evento duplicado | Prueba exitosa, documentada con las salidas de cada servicio | `docs/pruebas/prueba-recuperacion-kafka.md`, `docs/pruebas/capturas/` |
| 2026-11-16 | Matias Sinchi | Claude Code | Documentar en el README cómo ejecutar `cocina.py`, `caja.py` y `ventas.py` | Sección de ejecución con tabla de servicios | `readme.md` |
