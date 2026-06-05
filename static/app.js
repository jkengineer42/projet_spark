/* Logique JavaScript - Rendu du graphe avec Vis.js et polling de l'API */

let network = null;
let nodesDataSet = new vis.DataSet();
let edgesDataSet = new vis.DataSet();

// Configuration simple de l'affichage du graphe
const options = {
    nodes: {
        borderWidth: 1,
        borderWidthSelected: 2,
        font: {
            size: 11,
            face: 'Arial'
        }
    },
    edges: {
        arrows: {
            to: { enabled: true, scaleFactor: 0.6 }
        },
        smooth: {
            enabled: true,
            type: 'dynamic'
        }
    },
    physics: {
        enabled: true,
        barnesHut: {
            gravitationalConstant: -1200,
            springLength: 80
        }
    }
};

function init() {
    const container = document.getElementById('graph-container');
    const data = {
        nodes: nodesDataSet,
        edges: edgesDataSet
    };
    network = new vis.Network(container, data, options);
}

// Couleurs par défaut pour les types d'entités
function getColors(type) {
    if (type === 'User') {
        return { background: '#97c2fc', border: '#2b7ce9', highlight: { background: '#d2e5ff', border: '#2b7ce9' } };
    } else if (type === 'Seller') {
        return { background: '#a5e887', border: '#5cbf26', highlight: { background: '#c9f2b6', border: '#5cbf26' } };
    } else {
        return { background: '#ffd86e', border: '#e6a100', highlight: { background: '#ffe8a3', border: '#e6a100' } };
    }
}

// Configuration des liens selon le type de relation
function getEdgeProps(rel, price) {
    let color = '#7f8c8d';
    let dashes = false;
    let width = 1.3;
    let label = '';

    if (rel === 'AIME') {
        color = '#f1c40f';
        dashes = true;
        label = 'aime';
    } else if (rel === 'VOUT') {
        color = '#3498db';
        width = 2.2;
        label = 'vouloir acheter';
    } else if (rel === 'ACHAT') {
        color = '#e74c3c';
        width = 3.5;
        label = `achat (${price} €)`;
    } else if (rel === 'PROPOSE') {
        color = '#95a5a6';
        dashes = [5, 5];
        label = 'propose';
    }

    return { color, dashes, width, label };
}

// Appel de l'API FastAPI pour récupérer les données consolidées par Spark
async function refresh() {
    try {
        const res = await fetch('/api/graph');
        if (!res.ok) return;
        const data = await res.json();

        // Gestion du chargeur de démarrage
        const loader = document.getElementById('loader');
        if (data.vertices && data.vertices.length > 0) {
            if (loader) loader.style.display = 'none';
            document.getElementById('update-timer').innerText = `Mis à jour à ${new Date().toLocaleTimeString()}`;
        } else {
            if (loader) loader.style.display = 'flex';
            document.getElementById('update-timer').innerText = "Attente des données...";
            return;
        }

        // Mise à jour des compteurs statistiques
        const stats = data.stats || {};
        document.getElementById('kpi-total-interactions').innerText = stats.total_active_interactions || 0;
        document.getElementById('kpi-users').innerText = stats.active_users_count || 0;
        document.getElementById('kpi-sellers').innerText = stats.active_sellers_count || 0;
        document.getElementById('kpi-products').innerText = stats.active_products_count || 0;

        // Mise à jour incrémentale des nœuds
        const activeNodeIds = new Set();
        const nodesUpdate = data.vertices.map(v => {
            activeNodeIds.add(v.id);
            const degree = v.degree || 0;
            const size = 12 + Math.min(degree * 2, 20); // Taille indexée sur le degré de centralité
            return {
                id: v.id,
                label: v.id,
                title: v.label,
                shape: v.type === 'Product' ? 'box' : 'dot',
                size: v.type === 'Product' ? undefined : size,
                color: getColors(v.type),
                type: v.type,
                degree: degree
            };
        });

        const existingNodes = nodesDataSet.getIds();
        nodesDataSet.remove(existingNodes.filter(id => !activeNodeIds.has(id)));
        nodesDataSet.update(nodesUpdate);

        // Mise à jour incrémentale des arêtes
        const activeEdgeIds = new Set();
        const edgesUpdate = data.edges.map(e => {
            const id = `${e.src}_${e.dst}_${e.relationship}`;
            activeEdgeIds.add(id);
            const props = getEdgeProps(e.relationship, e.price);
            return {
                id: id,
                from: e.src,
                to: e.dst,
                label: props.label,
                color: { color: props.color, highlight: props.color },
                dashes: props.dashes,
                width: props.width
            };
        });

        const existingEdges = edgesDataSet.getIds();
        edgesDataSet.remove(existingEdges.filter(id => !activeEdgeIds.has(id)));
        edgesDataSet.update(edgesUpdate);

        // Remplissage du tableau de centralité
        const sorted = [...data.vertices].sort((a,b) => (b.degree || 0) - (a.degree || 0)).slice(0, 5);
        const tbody = document.getElementById('centrality-table-body');
        if (tbody) {
            if (sorted.length === 0) {
                tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;">Aucune donnée active</td></tr>`;
            } else {
                tbody.innerHTML = sorted.map(v => {
                    let badge = 'product';
                    let label = 'Produit';
                    if (v.type === 'User') { badge = 'user'; label = 'Acheteur'; }
                    else if (v.type === 'Seller') { badge = 'seller'; label = 'Vendeur'; }
                    return `
                        <tr>
                            <td>${v.id}</td>
                            <td><span class="badge ${badge}">${label}</span></td>
                            <td>${v.degree || 0}</td>
                        </tr>
                    `;
                }).join('');
            }
        }

    } catch (e) {
        console.error("Erreur de rafraîchissement:", e);
        document.getElementById('update-timer').innerText = "Erreur de connexion";
    }
}

document.addEventListener('DOMContentLoaded', () => {
    init();
    refresh();
    // Rafraîchissement périodique toutes les 3 secondes
    setInterval(refresh, 3000);
});
