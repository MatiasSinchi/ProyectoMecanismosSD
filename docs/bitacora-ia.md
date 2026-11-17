# Bitácora de uso de IA

Registro de las consultas hechas a herramientas de IA durante el proyecto. En el código, los fragmentos asistidos están marcados con el comentario `# Asistido por IA:`.

| Fecha | Integrante | Herramienta | Consulta | Resultado / uso | Archivos |
|-------|------------|-------------|----------|-----------------|----------|
| 2026-11-15 | Matias Sinchi | Claude Code | ¿Qué cambios introdujeron los últimos commits y qué problemas de diseño tienen? | Explicación de los avances (validación, Idempotency-Key, Kafka, Cocina, Caja, UUID) y de los puntos débiles: consumidores sin control de duplicados, campo `version` sin uso, estado sin paso intermedio | — |
| 2026-11-15 | Matias Sinchi | Claude Code | ¿Por qué no se instala `confluent-kafka` en Python 3.15 y cómo probar el flujo completo? | Explicación de que no hay wheel para 3.15 y hace falta `librdkafka`. Al probar el flujo se detectó que `cocina.py` y `caja.py` se caían si el topic aún no existía | — |
| 2026-11-15 | Matias Sinchi | Claude Code | ¿Por qué un consumidor falla con `UNKNOWN_TOPIC_OR_PART` y cómo evitarlo? | Explicación de que los topics se crean con el primer mensaje; propuesta de ignorar ese error al arrancar, aplicada en los consumidores. También se sugirió cómo documentar la ejecución en el README | `cocina.py`, `caja.py`, `readme.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Cómo implementar un consumidor de ventas con grupo propio, control de `eventId`, commit tras procesar y cierre con Control + C? | Explicación de los grupos de consumidores y del commit manual; propuesta de implementación usada como base para `ventas.py` | `ventas.py` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Cómo comprobar que Kafka conserva los pedidos cuando un servicio está caído? | Explicación de los offsets por grupo y de cómo consultarlos con `kafka-consumer-groups.sh`; guía de la prueba (detener Cocina, crear pedido, reiniciar) y apoyo para documentarla | `docs/pruebas/prueba-recuperacion-kafka.md`, `docs/pruebas/capturas/` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Qué información debe tener el README para ejecutar Cocina, Caja y Ventas? | Sugerencia de pasos por terminal y de una tabla de servicios, grupos y topics | `readme.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Por qué Cocina y Caja procesan dos veces el mismo evento y cómo evitarlo? | Explicación de la entrega *at-least-once* (commit después de procesar, rebalanceos, reintentos) y de los consumidores idempotentes; propuesta de control por `eventId` aplicada en ambos servicios | `cocina.py`, `caja.py`, `docs/pruebas/` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Qué debe incluir un documento de arquitectura y cómo representar el sistema con diagramas? | Explicación de las vistas de componentes, secuencia y despliegue, y de cómo hacer diagramas con Mermaid; apoyo en la redacción del documento | `docs/arquitectura.md`, `readme.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Qué es un ADR y por qué conviene REST frente a gRPC, GraphQL o WebSocket para la comunicación cliente–servicio? | Explicación del formato ADR (contexto, decisión, alternativas, consecuencias) y comparación de las alternativas; apoyo en la redacción del ADR | `docs/adr/ADR-001-rest.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Por qué usar Kafka entre servicios frente a REST, RabbitMQ o Redis? | Comparación de las alternativas: Kafka guarda los eventos y lleva un offset por grupo, mientras RabbitMQ borra el mensaje al consumirlo y Redis Pub/Sub no lo guarda; apoyo en la redacción del ADR | `docs/adr/ADR-002-kafka.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Qué mecanismos de sistemas distribuidos resuelven los reintentos, los duplicados, los IDs sin coordinación y el orden de eventos, y qué alternativas existen? | Explicación de *at-most-once*, *at-least-once* y *exactly-once*, Idempotency-Key, consumidores idempotentes, UUID frente a Snowflake, clave de partición, consistencia eventual frente a *two-phase commit* y *Transactional Outbox*; apoyo en la redacción del ADR | `docs/adr/ADR-003-seleccion-mecanismos.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Cómo comprobar que Idempotency-Key evita pedidos duplicados y en qué casos puede fallar? | Explicación de cómo verificar los eventos publicados con `kafka-get-offsets.sh` y de los casos límite (clave reutilizada con otro cuerpo, concurrencia, reinicio, Kafka caído). La prueba reveló una condición de carrera con peticiones simultáneas; apoyo en la documentación de la prueba | `docs/pruebas/prueba-idempotency-key.md`, `docs/pruebas/capturas/` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Cómo evitar que dos peticiones simultáneas con la misma Idempotency-Key creen dos pedidos? | Explicación de la condición de carrera entre comprobar y guardar la clave, y de por qué un candado por clave es mejor que uno global; propuesta de candado por clave, comparación del cuerpo (422) y respuesta 200 en reintentos, aplicada en `main.py` y verificada repitiendo la prueba | `main.py`, `docs/pruebas/prueba-idempotency-key.md`, `docs/adr/`, `docs/arquitectura.md` |
| 2026-11-16 | Matias Sinchi | Claude Code | ¿Qué aspectos conviene incluir en una reflexión sobre el uso de IA en el proyecto? | Sugerencia de estructura (uso, verificación, errores de la IA, qué no se delegó, aprendizajes) a partir del historial del proyecto; apoyo en la redacción | `docs/bitacora-ia.md` |

## Reflexión final

### Cómo se usó la IA

Se usó Claude Code en 14 consultas a lo largo del proyecto, sobre todo para tres cosas:

- **Entender conceptos** de sistemas distribuidos que aparecían al construir el sistema: grupos de consumidores y offsets en Kafka, entrega *at-least-once*, consumidores idempotentes, condiciones de carrera y consistencia eventual.
- **Diagnosticar problemas**, por ejemplo la caída de `cocina.py` y `caja.py` cuando el topic no existía, o por qué se duplicaban las facturas.
- **Apoyo en código y documentación**: propuestas de implementación (`ventas.py`, control de `eventId`, candado de `Idempotency-Key`) y apoyo en la redacción de la arquitectura, los ADR y las pruebas.

Los fragmentos de código que salieron de esas propuestas están marcados con `# Asistido por IA:`.

### Cómo se verificó lo que propuso la IA

Ninguna propuesta se dio por buena sin ejecutarla. Cada cambio se probó levantando Kafka y los servicios reales, y cada prueba quedó registrada con la salida de los programas en `docs/pruebas/capturas/`.

Esto fue importante porque las pruebas encontraron problemas que no se veían leyendo el código:

- **La prueba de recuperación** mostró que Cocina y Caja procesaban dos veces un evento repetido, aunque Ventas ya lo controlaba.
- **La prueba de `Idempotency-Key`** mostró que 10 peticiones simultáneas con la misma clave creaban 10 pedidos. El mecanismo parecía correcto en el código y funcionaba con reintentos uno a uno, pero no con peticiones en paralelo.

### Errores y límites de la IA observados

La IA también se equivocó, y por eso hubo que revisar sus resultados:

- Intentó abrir Docker Desktop cuando el equipo usa Colima, y usó `docker compose` cuando en esta instalación funciona `docker-compose`.
- En la prueba de `Idempotency-Key`, su primer script se quedó colgado y el caso del header vacío no probaba lo que decía (`curl` no envía un header con valor vacío). Se detectó al revisar la salida y se repitió el caso.
- Tiende a proponer soluciones más completas de lo necesario. Hubo que decidir qué se implementaba y qué quedaba solo documentado como limitación, por ejemplo guardar las claves en Redis.

### Qué no se delegó

- **Las decisiones:** qué requerimientos abordar, en qué orden y qué cambios aceptar.
- **Los commits:** se hicieron a mano después de revisar cada cambio, uno por cada avance.
- **Revisar el código del compañero:** los cambios a `main.py` se comunicaron al equipo porque cambian el comportamiento de la API.

### Aprendizajes

- **Un sistema distribuido falla por partes**, y el diseño tiene que asumirlo. Kafka permitió que la caída de la Cocina no detuviera a la Caja ni a Ventas, y que los pedidos pendientes se recuperaran.
- **No perder mensajes y no duplicarlos son objetivos que tiran en sentidos opuestos.** Confirmar después de procesar evita perder pedidos, pero obliga a que cada consumidor ignore los repetidos.
- **Un mecanismo que funciona en la prueba simple puede fallar con concurrencia.** Hay que probar los casos límite y no solo el camino feliz.
- **La IA acelera el aprendizaje y el desarrollo**, pero solo es útil si se entiende lo que propone y se verifica con pruebas reales.
