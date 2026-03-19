
## 2026 - 03 - 19

### import error
ik krijg deze error:

ImportError: cannot import name 'FileConfig' from 'imdb.defs.ingestion.file_configs' (/media/user/Data/dirkv/Code/dagster/dagster-workspace/projects/imdb/src/imdb/defs/ingestion/file_configs.py)

beetje raar, maar had ik denk ik eerder gehad

die import moet op een bepaalde manier vanwege het project

### assets aanroepen die door een factory gemaakt zijn

je kan ze als dependency importeren door gewoon een string te gebruiken

```python
@dg.asset(
    deps=["title_basics_raw", "title_ratings_raw"],  # String references
    group_name="data_analysis"
)
def total_line_count(context: dg.AssetExecutionContext):
    return 5
```