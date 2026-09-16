# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce
import pyarrow
from pyarrow import flight
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("env", "")
dbutils.widgets.text("dremio_username", "")
dbutils.widgets.text("dremio_personal_access_token", "")

# COMMAND ----------

env = dbutils.widgets.get("env").lower()
dremio_username = dbutils.widgets.get("dremio_username")
dremio_personal_access_token = dbutils.widgets.get("dremio_personal_access_token")

# COMMAND ----------

# MAGIC %run "./error_utils.py"

# COMMAND ----------

# MAGIC %run "./volume_util.py"

# COMMAND ----------

# MAGIC %run "./delta_table_utils.py"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create bronze.paam_clients table (Unity Catalog managed table)
# MAGIC REMOVED vs hive_metastore version: create_delta_table() with an explicit ADLS LOCATION. In UC we create a managed table (no LOCATION) under the catalog so storage is governed entirely by Unity Catalog.
# MAGIC

# COMMAND ----------

bronze_paam_clients_schema = StructType([
    StructField("source_system", StringType(), False),
    StructField("system_id", StringType(), False),  # Original source ID from PAAM
    StructField("counterparty_code", StringType(), False),
    StructField("client_display_name", StringType(), False),
    StructField("client_display_name_type", StringType(), True),
    StructField("client_display_name_top_level", StringType(), True),
    StructField("client_display_name_top_level_type", StringType(), True),
    StructField(
        "client_name_source",
        StringType(),
        False
    ),  # Party_Business_Alias or Ultimateparent_Business_Alias
    StructField("ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------

bronze_paam_clients_table = f"`{CATALOG_NAME}`.`bronze`.`paam_clients`"

columns_ddl = ",\n".join(
    f"`{field.name}` {field.dataType.simpleString()}"
    for field in bronze_paam_clients_schema.fields
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze_paam_clients_table} (
        {columns_ddl}
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all Trades in Silver after all daily pipelines are finished running
# MAGIC
# MAGIC REMOVED vs hive_metastore version: get_adls_base_path() + spark.read...load(path) for every silver table. Reading via catalog.schema.table names instead of raw ADLS paths.

# COMMAND ----------

silver_gfi_table = f"`{CATALOG_NAME}`.silver.fi_trades"
silver_x_dealer_table = f"`{CATALOG_NAME}`.silver.x_dealer"
silver_fx_pano_table = f"`{CATALOG_NAME}`.silver.fx_panoramic"
silver_cowen_table = f"`{CATALOG_NAME}`.silver.td_cowen_equities"
silver_us_prime_gen_table = f"`{CATALOG_NAME}`.silver.us_prime"
silver_cad_prime_monthly_table = f"`{CATALOG_NAME}`.silver.cad_prime_monthly"
silver_cad_prime_daily_table = f"`{CATALOG_NAME}`.silver.cad_prime_pnl_summary"

bronze_client_mappings_table = f"`{CATALOG_NAME}`.bronze.clients"

# COMMAND ----------

# MAGIC %md
# MAGIC #GFI
# MAGIC

# COMMAND ----------

gfi = spark.read.table(silver_gfi_table)

# COMMAND ----------

gfi = (
    gfi
    .withColumnRenamed("sourceSystem", "source_system")
    .withColumnRenamed("CounterPartyCode", "counterparty_code")
)


# COMMAND ----------

# MAGIC %md
# MAGIC # X -Dealer

# COMMAND ----------

x_dealer = spark.read.table(silver_x_dealer_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # CAD Prime Monthly

# COMMAND ----------

cad_prime_monthly = spark.read.table(silver_cad_prime_monthly_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # CAD PRIME Daily

# COMMAND ----------

cad_prime_daily = spark.read.table(silver_cad_prime_daily_table)

# COMMAND ----------

cad_prime_daily = (
    cad_prime_daily
    .withColumnRenamed("trade_system", "source_system")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # FX Pano

# COMMAND ----------

fx_pano = spark.read.table(silver_fx_pano_table)

# COMMAND ----------

fx_pano = (
    fx_pano
    .withColumnRenamed("sourceSystem", "source_system")
    .withColumnRenamed("murex_mnemonic", "counterparty_code")
)

# COMMAND ----------

#COWEN

# COMMAND ----------

cowen = spark.read.table(silver_cowen_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # US prime General

# COMMAND ----------

us_prime_gen = spark.read.table(silver_us_prime_gen_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine all trades

# COMMAND ----------

@enhanced_errors()
def concat_dataframes(dataframes):
    return reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True),
        dataframes
    )

# COMMAND ----------

trades_list = [
    gfi,
    x_dealer,
    fx_pano,
    cowen,
    cad_prime_monthly,
    cad_prime_daily,
    us_prime_gen,
]

# COMMAND ----------

trades = concat_dataframes(trades_list)

# COMMAND ----------

trades_unique_clients = (
    trades
    .dropna(subset=["source_system", "counterparty_code"])
    .select("source_system", "counterparty_code")
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Clients Table

# COMMAND ----------

clients = spark.read.table(bronze_client_mappings_table)

# COMMAND ----------



# COMMAND ----------

clients_extracted = clients.select(
    F.col("sourceSystem").alias("source_system"),
    F.col("shortCode").alias("counterparty_code"),
    F.col("client.displayName").alias("client_display_name"),
    F.col("client.type.title").alias("client_display_name_type"),
    F.col("client.toplevelClient.displayName").alias("client_display_name_top_level"),
    F.col("client.toplevelClient.type.title").alias("client_display_name_top_level_type"),
    F.col("client.focusedAccount").alias("focused_account"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Find all source_system counterparty_code combinations that are in Trades but not in Clients
# MAGIC

# COMMAND ----------

missing_mappings = trades_unique_clients.join(
    clients_extracted,
    on=["source_system", "counterparty_code"],
    how="anti",
)

# COMMAND ----------

# MAGIC %md
# MAGIC Query PaAM for missing Client Mappings \n
# MAGIC Dremio access itself is unrelated to Unity Catalog and is unchanged.

# COMMAND ----------

sql_query = (
    'SELECT * FROM TDS_DATA_MASTERS.PaAM."Party and Account Master".shortCodeQueryC360 '
    'WHERE Party_Business_Alias IS NOT NULL '
    'OR Ultimateparent_Business_Alias IS NOT NULL '
    'ORDER BY "system"'
)

# COMMAND ----------

dremio_url = {
    "dev": "valrsinfo-atgk1.dev.azure.td.com",
    "pat": "infoplatform-dremio-pat.corp.tdsecurities.com",
    "prod": "infoplatform-dremio.corp.tdsecurities.com",
}

# COMMAND ----------

port = 32010
client = flight.FlightClient(
    "grpc+tcp://" + dremio_url[env] + ":" + str(port)
)

# COMMAND ----------

with capture_errors(section="Connection to Dremio"):
    bearer_token = client.authenticate_basic_token(
        dremio_username,
        dremio_personal_access_token,
    )

# COMMAND ----------

options = flight.FlightCallOptions(headers=[bearer_token])

# COMMAND ----------

info = client.get_flight_info(
    flight.FlightDescriptor.for_command(sql_query),
    options,
)
reader = client.do_get(info.endpoints[0].ticket, options)

table = reader.read_all()
total_paam = spark.createDataFrame(table.to_pandas())

# COMMAND ----------

system_mappings = {
    "TOMS": [
        "XRS000001037",
        "XRS000001031",
        "XRS000001029",
        "XRS000001059",
        "XRS000001058",
        "XRS000001055",
        "XRS000001056",
        "XRS000001053",
        "XRS000001054",
    ],
    "ANVIL": ["XRS000001005"],
    "CALYPSO": [
        "XRS000001039",
        "XRS000001028",
        "XRS000001025",
        "XRS000001022",
    ],
    "ION": ["XRS000001002", "XRS000001041"],
    "GLOBAL": ["XRS000001014", "XRS000001007"],
    "MUREX_GLBFI": ["XRS000001027"],
    "COWEN": ["XRS000001019"],
}

# COMMAND ----------


missing_mappings.cache()
total_paam.cache()
filtered_paam_list = []

with capture_errors(section="Filter and build PaAM data"):
    for source_system, system_ids in system_mappings.items():
        party_list = [
            row.counterparty_code
            for row in (
                missing_mappings
                .filter(F.col("source_system") == source_system)
                .select("counterparty_code")
                .distinct()
                .collect()
            )
        ]

        if party_list:
            temp_df = (
                total_paam
                .filter(F.col("system").isin(system_ids))
                .filter(F.col("shortCode").isin(party_list))
            )
            filtered_paam_list.append(temp_df)

    missing_mappings.unpersist()
    total_paam.unpersist()

if filtered_paam_list:
    final_paam_clients = reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=False),
        filtered_paam_list,
    )

# COMMAND ----------

final_paam_clients = final_paam_clients.withColumn(
    "c360_source_system",
    F.lit(None),
)

for source_system, system_ids in system_mappings.items():
    final_paam_clients = final_paam_clients.withColumn(
        "c360_source_system",
        F.when(
            F.col("system").isin(system_ids),
            F.lit(source_system),
        ).otherwise(F.col("c360_source_system")),
    )

final_paam_clients = final_paam_clients.dropDuplicates(
    ["c360_source_system", "shortCode"]
)

# COMMAND ----------


final_paam_clients = (
    final_paam_clients
    .withColumn(
        "client_display_name",
        F.when(
            F.col("Party_Business_Alias").isNotNull(),
            F.col("Party_Business_Alias"),
        ).otherwise(F.col("Ultimateparent_Business_Alias")),
    )
    .withColumn(
        "client_display_name_type",
        F.when(
            F.col("Party_Business_Alias").isNotNull(),
            F.col("partyType"),
        ).otherwise(F.col("ultimateParent_PartyType")),
    )
    .withColumn(
        "client_display_name_top_level",
        F.when(
            F.col("Party_Business_Alias").isNotNull(),
            F.col("Ultimateparent_Business_Alias"),
        ).otherwise(F.lit(None)),
    )
    .withColumn(
        "client_display_name_top_level_type",
        F.when(
            F.col("Party_Business_Alias").isNotNull(),
            F.col("ultimateParent_PartyType"),
        ).otherwise(F.lit(None)),
    )
    .withColumn(
        "client_name_source",
        F.when(
            F.col("Party_Business_Alias").isNotNull(),
            F.lit("Party_Business_Alias"),
        ).otherwise(F.lit("Ultimateparent_Business_Alias")),
    )
)

# COMMAND ----------

final_paam_clients = (
    final_paam_clients
    .withColumnRenamed("c360_source_system", "source_system")
    .withColumnRenamed("system", "system_id")
    .withColumnRenamed("shortCode", "counterparty_code")
)

final_paam_clients = final_paam_clients.withColumn(
    "ingestion_timestamp",
    F.current_timestamp(),
)


# COMMAND ----------

@enhanced_errors()
def apply_schema_data_types(df, target_schema):
    existing_columns = set(df.columns)
    schema_dict = {
        field.name: field.dataType
        for field in target_schema.fields
    }

    for column_name, data_type in schema_dict.items():
        if column_name in existing_columns:
            df = df.withColumn(
                column_name,
                F.col(column_name).cast(data_type),
            )

    return df

# COMMAND ----------

final_paam_clients = apply_schema_data_types(
    final_paam_clients,
    bronze_paam_clients_schema,
)

# COMMAND ----------

schema_columns = [
    field.name
    for field in bronze_paam_clients_schema.fields
]

final_paam_clients = final_paam_clients.select(
    *[F.col(column) for column in schema_columns]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge into Unity Catalog table
# MAGIC REMOVED hive_metastore version: DeltaTable.forPath(spark, adls_path).Using DeltaTable.forName with the catalog-qualified table name instead,  since UC governs the table's storage location - the notebook no longer needs to know the underlying ADLS path at all.

# COMMAND ----------

current_paam_clients = DeltaTable.forName(spark, bronze_paam_clients_table)

# COMMAND ----------


(
    current_paam_clients.alias("current")
    .merge(
        final_paam_clients.alias("new"),
        (
            "current.source_system = new.source_system "
            "AND current.counterparty_code = new.counterparty_code"
        ),
    )
    .whenMatchedUpdate(
        condition=(
            F.col("current.client_name_source")
            != F.col("new.client_name_source")
        ),
        set={
            "client_display_name": "new.client_display_name",
            "client_display_name_type": "new.client_display_name_type",
            "client_display_name_top_level": "new.client_display_name_top_level",
            "client_display_name_top_level_type": "new.client_display_name_top_level_type",
            "client_name_source": "new.client_name_source",
        },
    )
    .whenMatchedUpdate(
        condition=(
            (
                (
                    F.col("current.client_display_name")
                    != F.col("new.client_display_name")
                )
                | (
                    F.col("current.client_display_name_top_level")
                    != F.col("new.client_display_name_top_level")
                )
            )
            & (
                F.col("current.client_name_source")
                == F.col("new.client_name_source")
            )
        ),
        set={
            "client_display_name": "new.client_display_name",
            "client_display_name_type": "new.client_display_name_type",
            "client_display_name_top_level": "new.client_display_name_top_level",
            "client_display_name_top_level_type": "new.client_display_name_top_level_type",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)