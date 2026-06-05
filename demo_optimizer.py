from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# 1. Initialisation de la SparkSession avec configuration de base
print("=== INITIALISATION DE LA SPARK SESSION ===")
spark = SparkSession.builder \
    .appName("DemoCatalystOptimizer") \
    .master("local[*]") \
    .getOrCreate()

# Réduire les logs pour éviter le bruit dans la console
spark.sparkContext.setLogLevel("WARN")

# ==========================================
# EXEMPLE 1 : ANALYSE DES PLANS (Slides 11-13)
# ==========================================
print("\n" + "="*60)
print("EXEMPLE 1 : REPRODUCTION DE L'APPLICATION DU COURS")
print("="*60)

# Données identiques aux slides
data = [
    ("Alice", 34, "Paris"),
    ("Bob", 45, "New York"),
    ("Cathy", 29, "Paris"),
    ("David", 42, "Lyon")
]

df = spark.createDataFrame(data, ["name", "age", "city"])

# Requête de groupe par ville avec comptage
df_grouped = df.groupBy("city").count()

print("\n--- EXPLAIN EXTENDED (Parsed, Analyzed, Optimized, Physical Plans) ---")
df_grouped.explain(mode="extended")


# ==========================================
# EXEMPLE 2 : CONSTANT FOLDING & BOOLEAN SIMPLIFICATION
# ==========================================
print("\n" + "="*60)
print("EXEMPLE 2 : CONSTANT FOLDING & BOOLEAN SIMPLIFICATION")
print("="*60)

# On applique un filtre avec : age > (20 + 10) AND true
# Catalyst doit plier (20 + 10) en 30 et ignorer le 'AND true'
df_opt_logical = df.filter((col("age") > (lit(20) + lit(10))) & lit(True))

print("\n--- PLAN LOGIQUE ET PHYSIQUE DU CONSTANT FOLDING ---")
df_opt_logical.explain(mode="extended")


# ==========================================
# EXEMPLE 3 : PREDICATE PUSHDOWN & PROJECTION PRUNING
# ==========================================
print("\n" + "="*60)
print("EXEMPLE 3 : PREDICATE PUSHDOWN & PROJECTION PRUNING")
print("="*60)

# On filtre sur l'âge > 30 puis on sélectionne uniquement le nom
# Catalyst doit :
# 1. Éliminer la colonne 'age' et 'city' pour le résultat final (Projection Pruning)
# 2. Garder 'age' uniquement pour le filtre, puis projeter uniquement 'name'
df_pushdown = df.filter(col("age") > 30).select("name")

print("\n--- PLAN LOGIQUE ET PHYSIQUE DU PUSHDOWN & PRUNING ---")
df_pushdown.explain(mode="extended")

# Arrêt de la SparkSession
spark.stop()
print("\n=== FIN DE LA DEMONSTRATION ===")
