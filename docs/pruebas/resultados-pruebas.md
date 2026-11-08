# Resultados de pruebas (verificados en un clúster local de procesos, NO en AWS)

Entorno: 3 MS + 5 DN como procesos en una sola máquina (127.0.0.1), sin TLS salvo la corrida mTLS indicada.
Las cifras de rendimiento NO son representativas de AWS Academy: repetir allí con `tests/performance`.

| Prueba | Resultado |
|---|---|
| functional + concurrency + replication (22 pruebas) | 22 pasaron |
| failures: lectura/escritura con un DN caído; re-replicación a R=3 | 2 pasaron (~130 s, incluye ~30 s de detección) |
| failures: caída del líder de metadatos | pasó; el cliente se recuperó en 9,3 s; lo confirmado se conservó |
| mTLS (certificados reales, 15 pruebas functional + replication) | 15 pasaron; cliente sin certificado rechazado |
| Rendimiento 50 MB (local) | put 25,1 MB/s, get 173,0 MB/s, metadatos (ls + locations) 2,3 ms |

No ejecutado: despliegue con Docker Compose/AWS (los archivos se validaron sintácticamente, no se levantaron),
pruebas con 100 MB/1 GB, variación de tamaño de bloque y de R, captura de tráfico (tcpdump).
