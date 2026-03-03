# MGD DWH

Sistema de Data Warehouse para Nexus. Incluye:

- **dwh_back**: API de configuración (FastAPI). Sirve configuraciones a los clientes.
- **dwh_client**: Cliente ETL que extrae de SQL Server y carga en MySQL DWH.
- **dwh_api**: Monitor API y CLI para monitorear ejecuciones y errores.

## Configuración

Copia `config.ini.example` como `config.ini` en cada carpeta y ajusta las credenciales.

## Requisitos

- Python 3.10+
- MySQL
- SQL Server (para el cliente)
- ODBC Driver for SQL Server
