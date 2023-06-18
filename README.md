# Apache Zeppelin plugin for storing notebooks in YDB

This plugin for [Apache Zeppelin](https://zeppelin.apache.org/) adds the capability to store Zeppelin notebooks in [YDB](https://ydb.tech) database.

The plugin supports notebook versioning through the explicit checkpoints in the Zeppelin UI.

Notebooks can be exported from YDB database to filesystem and imported from filesystem to YDB database as needed using the built-in utility program.

# Installation

## 1. Create the YDB database

The simplest option is to create a serverless YDB database, as specified in the [Yandex Cloud documentation](https://cloud.yandex.ru/docs/ydb/quickstart#create-db).

The database service page will contain the connection details, including the connection endpoint (typically `grpcs://ydb.serverless.yandexcloud.net:2135`) and database path (a value similar to `/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5`).

In order to access the created database, the [service account](https://cloud.yandex.ru/docs/iam/concepts/users/service-accounts) should be created, and configured with the `ydb.editor` role in the directory.

## 2. Create the database tables

Database tables should be created as specified in the [script provided](scripts/ydb-notebook-repo.sql). [YDB CLI](https://cloud.yandex.ru/docs/ydb/operations/connection) or [database UI in the Web Console](https://cloud.yandex.ru/docs/ydb/operations/crud#web-sql) can be used for running the SQL statements defining the data structures.

The schema of all the tables is defined as `zeppelin`. It can be changed to another value - e.g. for running multiple independant Zeppelin notebook stores in a single YDB database.

## 3. Install the plugin

The plugin JAR file (for example, `zeppelin-notebookrepo-ydb-1.0.jar`) should be put into the subdirectory `NotebookRepo/YdbNotebookRepo` of the Zeppelin's plugins directory (for example, `/usr/lib/zeppelin/plugins`).

```bash
sudo mkdirs /usr/lib/zeppelin/plugins/NotebookRepo/YdbNotebookRepo
sudo cp zeppelin-notebookrepo-ydb-1.0.jar /usr/lib/zeppelin/plugins/NotebookRepo/YdbNotebookRepo/
```

## 3. Enable the plugin

To enable the plugin, the following settings should be configured in the `zeppelin-site.xml` configuration file:

* `zeppelin.notebook.storage` should be set to `yandex.cloud.zeppelin.ydbrepo.YdbNotebookRepo`;
* `zeppelin.notebook.ydb.url` should be configured in the following format: `ENDPOINT?database=DBPATH`, where ENDPOINT is the database endpoint (including the protocol, hostname and port), and DBPATH is the database path;
* `zeppelin.notebook.ydb.dir` - schema name for the tables, usually defined as `zeppelin`;
* `zeppelin.notebook.ydb.auth.mode` - authentication mode, can be one of the following:
    * `METADATA` - virtual machine metadata in the cloud environment (e.g. service account linked to the VM where Zeppelin is running);
    * `SAKEY` - service account [authorization key file](https://cloud.yandex.ru/docs/iam/concepts/authorization/key);
    * `STATIC` - username and password;
    * `NONE` - anonymous access, if the YDB instance allows that (cannot be used for cloud-based YDB services).
* `zeppelin.notebook.ydb.auth.data` - authorization data, depending on the authentication mode chosen:
    * for `SAKEY` - local path to the service account authorization key file;
    * for `STATIC` - username and password pair, in the format of `username:password`.

The example configuration fragment in the `zeppelin-site.xml` file is shown below:

```xml
<configuration>
    
    <!-- Some other settings here -->
    
    <property>
        <name>zeppelin.notebook.storage</name>
        <value>yandex.cloud.zeppelin.ydbrepo.YdbNotebookRepo</value>
    </property>
    <property>
        <name>zeppelin.notebook.ydb.url</name>
        <value>grpcs://ydb.serverless.yandexcloud.net:2135?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5</value>
    </property>
    <property>
        <name>zeppelin.notebook.ydb.dir</name>
        <value>zeppelin</value>
    </property>
    <property>
        <name>zeppelin.notebook.ydb.auth.mode</name>
        <value>METADATA</value>
    </property>
</configuration>
```

## 4. Running the standalone import/export tool

The plugin contains the standalone import and export tool, which can be used to access the Zeppelin notebooks stored in YDB.

To run the tool, the configuration file is needed, containing the settings similar to those defined in the `zeppelin-site.xml` file. The example file is shown below:

```xml
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
    <comment>Example properties for YDB filesystem explorer tool</comment>
    <entry key="ydb.url">grpcs://ydb.serverless.yandexcloud.net:2135?database=/ru-central1/b1gfvslmokutuvt2g019/etnuogblap3e7dok6tf5</entry>
    <entry key="ydb.auth.mode">METADATA</entry>
    <entry key="ydb.dir">zeppelin</entry>
    <entry key="ydb.fname.mangle">true</entry>
</properties>
```

To import the notebooks from the specified directory, the following command can be used:

```bash
java -jar zeppelin-notebookrepo-ydb-1.0.jar config.xml import /var/lib/zeppelin/notebook
```


To export the notebooks from YDB to the specified directory, the following command can be used:

```bash
java -jar zeppelin-notebookrepo-ydb-1.0.jar config.xml export /tmp/notebooks
```
