# Proyecto de Trailers de Películas
Intro: La idea de este repo es permitir la predicción de éxito o fracaso de un estreno comercial basado en sus características y, especialmente, en los comentarios dejados por los usuarios de YouTube en el trailer oficial de la película. 

## Instrucciones para backup
Correr este bash desde el host:
`pg_dump -h localhost -p 5433 -U dbt_user analytics > backup_$(date +%Y%m%d).sql`
Para restaurar correr este comando: 
`psql -h localhost -p 5433 -U dbt_user analytics < backups/analytics_20240101_120000.sql` 
