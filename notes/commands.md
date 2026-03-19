dg scaffold defs dagster.asset assets.py



# Create all directories
mkdir -p src/my_project/defs/{ingestion,transform,warehouse,ml}

# Scaffold all asset files
dg scaffold defs dagster.asset ingestion/assets.py
dg scaffold defs dagster.asset transform/assets.py
dg scaffold defs dagster.asset warehouse/assets.py
dg scaffold defs dagster.asset ml/assets.py

# Scaffold all resource files (optional)
dg scaffold defs dagster.resource ingestion/resources.py
dg scaffold defs dagster.resource transform/resources.py
dg scaffold defs dagster.resource warehouse/resources.py
dg scaffold defs dagster.resource ml/resources.py
