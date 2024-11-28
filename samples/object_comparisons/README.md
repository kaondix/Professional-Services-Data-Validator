# DVT for Object Comparisons

DVT is not intended for object comparisons, it is a data validation tool. But
with some creativity it is easy to see how DVT could become part of an object
validation workflow when provided with adequate dictionary queries.

The files within this sample folder demonstrate the principle, SQL statements
may need modifying to suit individual needs.

The sample shell script `./reconcile.sh` shows how the files can be executed using DVT, for example:

```
./reconcile.sh oracle postgres
```