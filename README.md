# Apache Zeppelin plugin for storing notebooks in YDB

This plugin for [Apache Zeppelin](https://zeppelin.apache.org/) adds the capability to store Zeppelin notebooks in [YDB](https://ydb.tech) database.

The plugin supports notebook versioning through the explicit checkpoints in the Zeppelin UI.

Notebooks can be exported from YDB database to filesystem and imported from filesystem to YDB database as needed using the built-in utility program.

# Installation

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


```bash
java -jar /usr/lib/zeppelin/plugins/NotebookRepo/YdbNotebookRepo/zeppelin-notebookrepo-ydb-1.0-SNAPSHOT.jar /tmp/ydb.xml import /var/lib/zeppelin/notebook
```

```xml
<configuration>
<!-- ***** -->
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