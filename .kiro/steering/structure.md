# Estructura del proyecto

## Árbol de carpetas

```
serverless-datalake-aws/
├── app.py                      # Punto de entrada CDK: instancia DataLakeStack
├── datalake/
│   └── datalake_stack.py       # Único stack con toda la infraestructura
├── lambda_src/
│   └── trigger_pipeline.py     # Handler Lambda que inicia el Glue Job
├── glue_src/
│   └── transform_job.py        # Script del Glue Job (CSV -> Parquet)
├── sample_data/
│   └── generate_ventas.py      # Generador standalone de CSV de ventas ficticias
├── tests/
│   └── test_datalake_stack.py  # Tests con CDK assertions
├── requirements.txt
├── .gitignore
├── LICENSE                     # MIT
└── README.md
```

## Convenciones de código

- **Nombres de recursos en inglés** (clases, variables, IDs de constructs, nombres de recursos AWS).
- **Comentarios en español** — proyecto educativo para audiencia hispanohablante.
- **Docstrings en todas las funciones**, en español, explicando el porqué además del qué.
- Un solo propósito por archivo; nombres descriptivos.

## Convenciones de infraestructura

- Todo dentro de `DataLakeStack`.
- `RemovalPolicy.DESTROY` y `auto_delete_objects=True` en el bucket (proyecto educativo, debe
  poder destruirse limpio).
- Roles IAM de **mínimo privilegio**: cada servicio recibe solo los permisos que necesita, sin
  wildcards de recursos.
- Outputs del stack para los identificadores que el usuario necesita después del deploy.

## README (en español)

Debe incluir:

- Diagrama de arquitectura.
- Prerequisitos.
- Instrucciones de deploy (`cdk bootstrap` + `cdk deploy`).
- Cómo probar el pipeline de punta a punta.
- Costos estimados.
- Cómo destruir el stack (`cdk destroy`).

## Flujo de trabajo con Git

- **Commits frecuentes**: un commit (y push) al completar cada task del spec.
- Formato de mensajes **convencional**: `feat:`, `fix:`, `test:`, `docs:`.
- El **primer commit** incluye la estructura base del proyecto junto con los steering files y el
  spec, para que el historial cuente la historia completa.
- Si una task implica varios archivos relacionados (p. ej. el Glue Job y su rol IAM), van en el
  **mismo commit**; nunca mezclar tasks distintas en un commit.
