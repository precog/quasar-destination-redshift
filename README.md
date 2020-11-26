# quasar-destination-redshift [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-destination-redshift" % <version>
```

## Configuration

This implementation stages files to S3 before loading them into
Redshift. Because of this you'll need to provide configuration for Quasar to
upload the bytestream to S3 and configuration for Redshift to download
the bytestream from S3. This might imply *two* different set of
keys. Format:

```json
{
  "bucket": Object,
  "jdbcUri": String,
  "user": String,
  "password": String
  "authorization": Object
}
```

- `bucket` contains an object specifying the staging bucket. It has the
following format:

```json
{
  "bucket": String,
  "accessKey": String,
  "secretKey": String,
  "region": String
}
```

The inner `bucket` key is the *name* (not URL) of the bucket used to upload.

- `jdbcUri`is the URI used connect to the Redshift cluster eg. `jdbc:redshift://redshift-cluster-1.example.outer-space.redshift.amazonaws.com:5439/dev`

- `user` is the username for the cluster, typically `awsuser`

- `password` is the password for the same username

- `authorization` is an object specifying how the Redshift cluster
will connect to the staging bucket. If using roles, the object takes
the form:

```json
{
  "type": "role",
  "arn": String,
  "region": String
}
```

`arn` is the Amazon Resource Name for the role attached to the
Redshift cluster. 

`region` is the region where the staging bucket is. This is important
for cross-region loading

If using `keys` based authorization. The object takes the form:

```json
{
  "type": "keys",
  "accessKey": String,
  "secretKey": String,
  "region": String
}
```

`region` is the region where the staging bucket is. This is important
for cross-region loading

