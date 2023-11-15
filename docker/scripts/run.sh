#!/usr/bin/env bash

set -euxo pipefail

generate_database_config(){
  cat << XML
<property>
  <name>javax.jdo.option.ConnectionDriverName</name>
  <value>org.postgresql.Driver</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionURL</name>
  <value>jdbc:postgresql://postgres:5432/metastore_db</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionUserName</name>
  <value>admin</value>
</property>
<property>
  <name>javax.jdo.option.ConnectionPassword</name>
  <value>admin</value>
</property>
<property>
    <name>metastore.task.threads.always</name>
    <value>org.apache.hadoop.hive.metastore.events.EventCleanerTask</value>
</property>
<property>
    <name>metastore.expression.proxy</name>
    <value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value>
</property>

<property>
    <name>metastore.thrift.port</name>
    <value>9083</value>
</property>
XML
}

generate_hive_site_config(){
  database_config=$(generate_database_config)
  cat << XML > "$1"
<configuration>
$database_config
</configuration>
XML
}


run_migrations(){
  if /opt/hive-metastore/bin/schematool -dbType "$DATABASE_TYPE" -validate | grep 'Done with metastore validation' | grep '[SUCCESS]'; then
    echo 'Database OK'
    return 0
  else
    # TODO: how to apply new version migrations or repair validation issues
    /opt/hive-metastore/bin/schematool --verbose -dbType "$DATABASE_TYPE" -initSchema
  fi
}

# configure & run schematool
generate_hive_site_config /opt/hadoop/etc/hadoop/hive-site.xml
run_migrations

# configure & start metastore (in foreground)

/opt/hive-metastore/bin/start-metastore