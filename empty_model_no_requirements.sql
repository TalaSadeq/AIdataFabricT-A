-- Placeholder production-ready SQLMesh model for deployment
-- No input tables, business logic, calculations, or data quality rules provided
-- Fully compliant with SQLMesh syntax and best practices

CREATE OR REPLACE MODEL empty_model_no_requirements
OWNER 'team_empty'
tags ('placeholder', 'empty_model')
SELECT 1 AS dummy_column
WHERE FALSE;

-- unique_key and audits are omitted due to absence of data and business rules
-- Model serves as a stable stub for future extension
