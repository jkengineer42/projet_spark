import os
import json
import shutil
import datetime

# Forcer l'utilisation de Java 11 pour éviter les erreurs getSubject sur Java 24/25
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, window

INPUT_DIR = "data/input_stream"
OUTPUT_DIR = "data/output_graph"
STATE_FILE = os.path.join(OUTPUT_DIR, "graph_state.json")
TEMP_FILE = os.path.join(OUTPUT_DIR, "graph_state.tmp")

# Initialisation de la session Spark
print("Initialisation de PySpark...")
spark = SparkSession.builder \
    .appName("ProjetSparkStreamingGraph") \
    .master("local[*]") \
    .getOrCreate()

# Réduire le niveau des logs
spark.sparkContext.setLogLevel("WARN")

# Définition du schéma pour le flux (Schema Enforcement)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True)
])

# Lecture du flux JSON
print(f"Lecture du flux depuis '{INPUT_DIR}'...")
streaming_df = spark.readStream \
    .schema(schema) \
    .json(INPUT_DIR)

# Requête 1 : Agrégation par fenêtre glissante (5 min de fenêtre, 1 min de glissement)
# Le watermark permet de rejeter les données en retard de plus de 10 min
windowed_df = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("action_type")
    ) \
    .count()

query_aggregates = windowed_df.writeStream \
    .format("memory") \
    .queryName("action_aggregates") \
    .outputMode("complete") \
    .start()

# Requête 2 : Traitement personnalisé du graphe et écriture du fichier JSON
def process_batch(batch_df, batch_id):
    print(f"\n--- Batch #{batch_id} ---")
    if batch_df.count() == 0:
        print("Pas de nouvelles données.")
        return
        
    # On récupère le timestamp le plus récent du batch pour définir notre fenêtre glissante
    max_time_row = batch_df.agg({"timestamp": "max"}).collect()
    max_time = max_time_row[0][0]
    if not max_time:
        return
        
    # Filtrage temporel pour ne garder que les 5 dernières minutes de données actives
    limit_time = max_time - datetime.timedelta(minutes=5)
    active_df = batch_df.filter(col("timestamp") >= limit_time).cache()
    active_count = active_df.count()
    print(f"Nombre d'événements actifs : {active_count}")
    
    if active_count == 0:
        active_df.unpersist()
        return
        
    # 1. Extraction des sommets (Vertices)
    users = active_df.select("user_id", "user_city").distinct() \
        .withColumnRenamed("user_id", "id") \
        .withColumn("type", lit("User")) \
        .withColumn("label", concat(lit("Acheteur ("), col("user_city"), lit(")")) ) \
        .select("id", "type", "label")
        
    sellers = active_df.select("seller_id").distinct() \
        .withColumnRenamed("seller_id", "id") \
        .withColumn("type", lit("Seller")) \
        .withColumn("label", lit("Vendeur")) \
        .select("id", "type", "label")
        
    products = active_df.select("product_id", "product_cat").distinct() \
        .withColumnRenamed("product_id", "id") \
        .withColumn("type", lit("Product")) \
        .withColumn("label", col("product_cat")) \
        .select("id", "type", "label")
        
    vertices_df = users.union(sellers).union(products).distinct()
    
    # 2. Extraction des liens (Edges)
    # Liens Utilisateur -> Produit (Like, Intent, Achat)
    user_edges = active_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship"),
        col("price")
    )
    
    # Liens Vendeur -> Produit (Propose)
    seller_edges = active_df.select(
        col("seller_id").alias("src"),
        col("product_id").alias("dst"),
        lit("PROPOSE").alias("relationship"),
        col("price")
    ).distinct()
    
    edges_df = user_edges.union(seller_edges)
    
    # 3. Calcul de la centralité (degré entrant, sortant et total)
    out_deg = edges_df.groupBy("src").count().withColumnRenamed("src", "id").withColumnRenamed("count", "out")
    in_deg = edges_df.groupBy("dst").count().withColumnRenamed("dst", "id").withColumnRenamed("count", "in")
    
    vertices_metrics = vertices_df \
        .join(in_deg, "id", "left") \
        .join(out_deg, "id", "left") \
        .na.fill(0) \
        .withColumn("degree", col("in") + col("out"))
        
    # Récupération des données sous forme de listes Python
    vertices_list = [row.asDict() for row in vertices_metrics.collect()]
    edges_list = [row.asDict() for row in edges_df.collect()]
    
    # Statistiques du batch
    stats = {
        "timestamp": datetime.datetime.now().isoformat(),
        "total_active_interactions": active_count,
        "active_users_count": users.count(),
        "active_sellers_count": sellers.count(),
        "active_products_count": products.count()
    }
    
    graph_state = {
        "stats": stats,
        "vertices": vertices_list,
        "edges": edges_list
    }
    
    # Écriture propre du fichier de graphe pour le serveur web
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    try:
        with open(TEMP_FILE, "w", encoding="utf-8") as f:
            json.dump(graph_state, f, indent=2)
        shutil.move(TEMP_FILE, STATE_FILE)
        print(f"Graphe enregistré ({len(vertices_list)} sommets, {len(edges_list)} liens).")
    except Exception as e:
        print(f"Erreur d'écriture : {e}")
        
    active_df.unpersist()
    
    # Affichage rapide des volumes par fenêtre
    spark.sql("SELECT window.start, window.end, action_type, count FROM action_aggregates ORDER BY start DESC LIMIT 5").show(truncate=False)

query_graph = streaming_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

try:
    print("Moteur de streaming démarré. En attente des données...")
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nArrêt...")
finally:
    query_aggregates.stop()
    query_graph.stop()
    spark.stop()
