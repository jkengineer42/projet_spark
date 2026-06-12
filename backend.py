import os
import json
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
zizi

app = FastAPI(title="Projet Big Data - Serveur API")

STATE_FILE = "data/output_graph/graph_state.json"

@app.get("/")
def read_index():
    index_path = "static/index.html"
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"error": "Fichier static/index.html introuvable"}

@app.get("/api/graph")
def get_graph_state():
    """Renvoie le dernier état du graphe généré par Spark"""
    if not os.path.exists(STATE_FILE):
        # Si Spark n'a pas encore créé de fichier, on renvoie une structure vide
        return {
            "stats": {
                "timestamp": None,
                "total_active_interactions": 0,
                "active_users_count": 0,
                "active_sellers_count": 0,
                "active_products_count": 0
            },
            "vertices": [],
            "edges": []
        }
    
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de lecture : {str(e)}")

# Chargement du dossier contenant les fichiers statiques (CSS/JS)
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    print("Démarrage du serveur web de rendu...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
