# Athena Migrator

This sample project allows you to reconstruct a athena database by storing the migrations applied to a database in the same order. This allow you to keep environments in sync and perform safe migrations.

You can use it in a lambda function by adding the needed iam permissions: (both to Athena and S3 storage).

It basically instantiates an athena context in the same region that you are and apply the scripts stored in the /data/scripts directory.

```
/data/scripts
|___202206211901-create-db-refined.hql
|___202206211935-create-table-test.hql
```

And then you also need to update the following method on the `athena_migrator.migrations` module:

```
def get_migrations(BUCKET: str):
    return [
        ParametrizedQuery('202206211901-create-db-refined.hql'),
        ParametrizedQuery('202206211935-create-table-test.hql', {'#BUCKET': BUCKET})
    ]
```

The program will check the already launched migrations and after executing each one it will store it in an inner table so you can query it from athena! It works like: [DbUp](https://dbup.readthedocs.io/en/latest/)


