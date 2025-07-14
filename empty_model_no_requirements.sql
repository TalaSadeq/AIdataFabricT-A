-- Placeholder production-ready SQLMesh model for deployment
-- No input tables, business logic, calculations, or data quality rules provided
-- Fully compliant with SQLMesh best practices

CREATE OR REPLACE MODEL empty_model_no_requirements
OWNER 'team_empty'
tags ('placeholder', 'empty_model')
SELECT 1 AS dummy_column
WHERE FALSE;

-- No unique_key or audits specified due to absence of data and rules
-- Model serves as a stable stub for future development
