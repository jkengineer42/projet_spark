import os
import time
import json
import random
import glob
from datetime import datetime, timezone

# Dossier d'entrée pour Spark Streaming
INPUT_DIR = "data/input_stream"
MAX_FILES = 200  # On garde un nombre suffisant de fichiers pour que Spark ait le temps de les lire

# Données pour la simulation
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Bordeaux", "Nantes", "Lille"]
CATEGORIES = {
    "Vehicules": (500, 15000),
    "Immobilier": (60000, 250000),
    "Mode": (10, 100),
    "Maison": (15, 500),
    "Electronique": (40, 1000)
}
ACTIONS = ["AIME", "VOUT", "ACHAT"]

# Pool fixe pour simuler des utilisateurs récurrents et des produits
USER_IDS = [f"usr_{random.randint(100, 999)}" for _ in range(30)]
SELLER_IDS = [f"sel_{random.randint(10, 99)}" for _ in range(10)]
PRODUCTS = []

for i in range(15):
    cat = random.choice(list(CATEGORIES.keys()))
    min_p, max_p = CATEGORIES[cat]
    PRODUCTS.append({
        "product_id": f"prod_{100 + i}",
        "product_cat": cat,
        "price": round(random.uniform(min_p, max_p), 2),
        "seller_id": random.choice(SELLER_IDS)
    })

def clean_old_files():
    """Supprime les fichiers les plus anciens pour faire de la place"""
    files = sorted(glob.glob(os.path.join(INPUT_DIR, "events_*.json")), key=os.path.getmtime)
    if len(files) > MAX_FILES:
        for f in files[:-MAX_FILES]:
            try:
                os.remove(f)
            except Exception:
                pass

def main():
    print(f"Lancement du simulateur d'événements. Fichiers écrits dans '{INPUT_DIR}'")
    
    # Nettoyage initial du dossier
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)
    for f in glob.glob(os.path.join(INPUT_DIR, "events_*.json")):
        try:
            os.remove(f)
        except Exception:
            pass
            
    while True:
        try:
            # Génération d'un lot d'événements (entre 1 et 4)
            nb_events = random.randint(1, 4)
            events = []
            
            for _ in range(nb_events):
                prod = random.choice(PRODUCTS)
                user_id = random.choice(USER_IDS)
                city = random.choice(CITIES)
                action = random.choice(ACTIONS)
                
                event = {
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "user_id": user_id,
                    "user_city": city,
                    "product_id": prod["product_id"],
                    "product_cat": prod["product_cat"],
                    "seller_id": prod["seller_id"],
                    "action_type": action,
                    "price": prod["price"]
                }
                events.append(event)
            
            # Écriture sous format JSON Lines (un JSON par ligne)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            file_path = os.path.join(INPUT_DIR, f"events_{timestamp}.json")
            
            with open(file_path, "w", encoding="utf-8") as f:
                for ev in events:
                    f.write(json.dumps(ev) + "\n")
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Génération de {nb_events} événements dans {os.path.basename(file_path)}")
            
            clean_old_files()
            
            # Attente de 2 secondes
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\nArrêt du simulateur.")
            break
        except Exception as e:
            print(f"Erreur dans le simulateur : {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
