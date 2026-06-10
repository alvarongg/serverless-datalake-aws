"""Paquete de infraestructura CDK del data lake serverless.

Contiene la definición del único stack (`DataLakeStack`). Mantener este paquete
separado del código de runtime (Lambda y Glue Job) refuerza la regla del
proyecto: la definición de recursos no se mezcla con la lógica de ejecución.
"""
