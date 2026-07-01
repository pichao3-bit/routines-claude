#!/usr/bin/env python3
"""
veille_bodacc_rss.py

Veille juridique/économique locale (Vaucluse 84, Gard 30, Bouches-du-Rhône 13),
ciblée sur le secteur AGROALIMENTAIRE (production, transformation, viticulture,
négoce agricole...), pour repérer tôt les procédures collectives, ventes/
cessions de fonds, et changements de dirigeant, en combinant :
  1. L'API ouverte BODACC (DILA / data.gouv.fr) - gratuite, sans clé - filtrée
     par mots-clés sectoriels sur le texte de l'activité déclarée.
  2. Les flux RSS de la presse économique régionale (filtrés par mots-clés
     sectoriels + événement) et de la presse spécialisée agroalimentaire
     nationale (filtrée par mots-clés géographiques).

Toutes les URLs, noms de champs et syntaxes de requête ci-dessous ont été
vérifiés en direct (requêtes réelles) le 2026-07-01. Voir README.md pour le
détail de ce qui a été testé, les limites du filtrage par mots-clés (pas de
code NAF/APE structuré dans ce jeu de données BODACC), et ce qui reste à
ajuster.

Sortie : un JSON structuré + un digest texte, sur stdout ou dans des fichiers
si --out-json / --out-text sont fournis.

Dépendances : requests, feedparser
    pip install requests feedparser
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import feedparser
import requests

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

DEPARTEMENTS = ["84", "30", "13"]  # Vaucluse, Gard, Bouches-du-Rhône
LOOKBACK_DAYS_DEFAULT = 3

BODACC_API_URL = (
    "https://bodacc-datadila.opendatasoft.com/api/records/1.0/search/"
)
BODACC_DATASET = "annonces-commerciales"

# Familles d'avis BODACC réellement disponibles sur ce dataset (vérifié via
# l'endpoint facet=familleavis). Le nom affiché est familleavis_lib.
#   collective    -> "Procédures collectives"
#   vente         -> "Ventes et cessions"
#   modification  -> "Modifications diverses" (inclut changement de gérant/
#                    président, mais aussi capital, adresse, etc. - PAS un
#                    filtre "nominations" propre. On l'affine avec des
#                    mots-clés ci-dessous.)
BODACC_FAMILLES = ["collective", "vente", "modification"]

# Mots-clés pour ne garder, dans la famille "modification", que les avis qui
# ressemblent à un changement de dirigeant (le champ concerné est
# modificationsgenerales.descriptif).
DIRIGEANT_KEYWORDS = [
    "gérant", "gerant", "président", "president", "directeur général",
    "directeur general", "administrateur", "gérance", "gerance",
]

# Mots-clés sectoriels "agroalimentaire" (production, transformation,
# viticulture, négoce agricole/alimentaire...). Utilisés :
#  - côté BODACC : en recherche plein texte (q=) sur l'ensemble du document
#    (dénomination, activité déclarée, etc.) - vérifié le 2026-07-01 que ça
#    matche bien le champ activité imbriqué (listeetablissements/
#    listepersonnes), y compris les expressions avec apostrophe.
#  - côté presse régionale généraliste : combiné en ET avec RSS_KEYWORDS.
# Limite connue : BODACC n'a pas de code NAF/APE structuré et filtrable dans
# ce jeu de données -> ce filtrage par mots-clés est une approximation. Des
# entreprises agroalimentaires qui ne décrivent pas leur activité avec ces
# termes précis peuvent être ratées (faux négatifs), et à l'inverse un terme
# comme "brasserie" peut désigner un bar/restaurant plutôt qu'un brasseur de
# bière (faux positif) - volontairement restreint à "brasserie de bière" /
# "fabrication de bière" pour limiter ce risque. Ajuste cette liste selon ce
# que tu vois remonter après quelques exécutions.
SECTOR_KEYWORDS = [
    "agroalimentaire", "agro-alimentaire", "industrie alimentaire",
    "transformation alimentaire", "produits alimentaires",
    "viticulture", "vinicole", "cave viticole", "cave coopérative",
    "domaine viticole", "riziculture", "riz de Camargue",
    "oléiculture", "huile d'olive", "huilerie",
    "maraîchage", "arboriculture fruitière",
    "conserverie", "confiserie", "distillerie",
    "brasserie de bière", "fabrication de bière", "microbrasserie",
    "abattoir", "élevage", "meunerie",
    "coopérative agricole", "coopérative viticole",
    "négoce agricole", "négoce alimentaire",
    "conditionnement alimentaire", "fruits et légumes",
    "boulangerie industrielle", "boucherie", "charcuterie", "fromagerie",
]

# Mots-clés géographiques : utilisés uniquement pour la presse agroalimentaire
# NATIONALE (RIA, voir plus bas), qui elle est déjà 100% sectorielle mais pas
# du tout limitée à notre zone - il faut donc filtrer par lieu plutôt que par
# secteur pour ce flux-là.
REGION_KEYWORDS = [
    "vaucluse", "gard", "bouches-du-rhône", "bouches du rhône",
    "avignon", "nîmes", "nimes", "marseille", "aix-en-provence",
    "aix en provence", "arles", "alès", "ales", "carpentras",
    "cavaillon", "orange", "salon-de-provence", "istres", "martigues",
    "provence", "camargue",
]

# Mots-clés "événement" pour filtrer les flux RSS de presse généraliste (ils
# ne sont pas limités à l'économie/justice, sauf quand une catégorie dédiée
# existe). Pour ces flux, un article n'est retenu que s'il matche à la fois
# un mot-clé ici ET un mot-clé sectoriel (SECTOR_KEYWORDS) - voir parse_rss().
RSS_KEYWORDS = [
    "liquidation", "redressement judiciaire", "cessation d'activité",
    "cessation d'activite", "cession", "reprise", "rachat", "nomination",
    "nouveau dirigeant", "prend la direction", "prend la présidence",
    "succède à", "succede a", "dépôt de bilan", "depot de bilan",
    "sauvegarde judiciaire", "tribunal de commerce", "mise en vente",
    "fermeture", "plan social",
]

# Flux RSS vérifiés en direct le 2026-07-01 (statut HTTP 200, contenu RSS
# valide relu). Voir README.md pour le détail de la vérification et pour
# La Provence Éco (non confirmé, volontairement absent d'ici).
#
# scope:
#  - "regional_generalist" : presse économique locale généraliste, pas
#    limitée à l'agroalimentaire -> on filtre par SECTOR_KEYWORDS ET
#    RSS_KEYWORDS (event).
#  - "sector_national" : presse spécialisée agroalimentaire, déjà 100%
#    sectorielle mais nationale -> on filtre par REGION_KEYWORDS (lieu),
#    et on marque en plus si ça matche aussi un mot-clé "événement" pour
#    prioriser dans le digest.
RSS_FEEDS = [
    {
        "name": "L'Echo du Mardi - Economie",
        "url": "https://www.echodumardi.com/category/economie/feed",
        "zone": "84",
        "scope": "regional_generalist",
    },
    {
        "name": "Objectif Gard - Economie",
        "url": "https://www.objectifgard.com/economie/feed",
        "zone": "30",
        "scope": "regional_generalist",
    },
    {
        "name": "Le Journal des Entreprises - Occitanie",
        "url": "https://www.lejournaldesentreprises.com/rss-occitanie",
        "zone": "30",
        "scope": "regional_generalist",
    },
    {
        "name": "Le Journal des Entreprises - Région Sud",
        "url": "https://www.lejournaldesentreprises.com/rss-region-sud",
        "zone": "84/13",
        "scope": "regional_generalist",
    },
    {
        "name": "RIA - L'actualité de l'industrie agroalimentaire",
        "url": "https://www.ria.fr/rss",
        "zone": "national (filtré par lieu)",
        "scope": "sector_national",
    },
]

REQUEST_TIMEOUT = 20
USER_AGENT = "veille-transition-bot/1.0 (+contact: pichao3@gmail.com)"


# --------------------------------------------------------------------------
# BODACC
# --------------------------------------------------------------------------

def build_sector_query(date_from: str, date_to: str) -> str:
    """Construit la clause q= combinant le filtre sectoriel agroalimentaire
    (OR entre tous les mots-clés, en phrase exacte pour les expressions à
    plusieurs mots) et la fenêtre de dates, vérifiée le 2026-07-01 sur
    l'API BODACC (opendatasoft, syntaxe façon Lucene)."""
    clauses = [
        f'"{kw}"' if " " in kw or "'" in kw else kw
        for kw in SECTOR_KEYWORDS
    ]
    sector_clause = " OR ".join(clauses)
    return f"({sector_clause}) AND dateparution:[{date_from} TO {date_to}]"


def fetch_bodacc(dept: str, famille: str, date_from: str, date_to: str,
                  rows: int = 50) -> list[dict[str, Any]]:
    """Interroge l'API BODACC pour un département et une famille d'avis
    donnés, sur une fenêtre de dates [date_from, date_to] (YYYY-MM-DD),
    restreinte au secteur agroalimentaire (SECTOR_KEYWORDS).
    Retourne la liste des 'fields' de chaque enregistrement.
    """
    params = {
        "dataset": BODACC_DATASET,
        "q": build_sector_query(date_from, date_to),
        "refine.numerodepartement": dept,
        "refine.familleavis": famille,
        "rows": rows,
        "sort": "-dateparution",
    }
    try:
        resp = requests.get(
            BODACC_API_URL, params=params,
            headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[BODACC] Erreur dept={dept} famille={famille}: {exc}",
              file=sys.stderr)
        return []

    data = resp.json()
    return [rec["fields"] for rec in data.get("records", [])]


def matches_dirigeant_keywords(fields: dict[str, Any]) -> bool:
    """Pour la famille 'modification', ne garde que ce qui ressemble à un
    changement de dirigeant (approximatif : basé sur le texte descriptif)."""
    raw = fields.get("modificationsgenerales", "")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = {}
    descriptif = (raw.get("descriptif", "") if isinstance(raw, dict) else "")
    descriptif_lower = descriptif.lower()
    return any(kw in descriptif_lower for kw in DIRIGEANT_KEYWORDS)


def collect_bodacc(date_from: str, date_to: str) -> list[dict[str, Any]]:
    results = []
    for dept in DEPARTEMENTS:
        for famille in BODACC_FAMILLES:
            for fields in fetch_bodacc(dept, famille, date_from, date_to):
                if famille == "modification" and not matches_dirigeant_keywords(fields):
                    continue
                results.append({
                    "source": "BODACC",
                    "departement": fields.get("departement_nom_officiel"),
                    "numerodepartement": fields.get("numerodepartement"),
                    "famille": fields.get("familleavis_lib"),
                    "date_parution": fields.get("dateparution"),
                    "entreprise": fields.get("commercant"),
                    "ville": fields.get("ville"),
                    "tribunal": fields.get("tribunal"),
                    "url": fields.get("url_complete"),
                    "id": fields.get("id"),
                })
    return results


# --------------------------------------------------------------------------
# RSS presse régionale
# --------------------------------------------------------------------------

def parse_rss(feed: dict[str, str], since: datetime) -> list[dict[str, Any]]:
    """Récupère et filtre un flux RSS : garde les items publiés après `since`
    et pertinents selon le "scope" du flux (voir RSS_FEEDS) :
      - regional_generalist : mot-clé sectoriel ET mot-clé événement requis.
      - sector_national      : mot-clé géographique requis (le secteur est
        déjà garanti par la nature du flux) ; le mot-clé événement, s'il est
        présent en plus, sert juste à marquer l'article "signal fort".
    """
    try:
        resp = requests.get(
            feed["url"], headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[RSS] Erreur flux '{feed['name']}' ({feed['url']}): {exc}",
              file=sys.stderr)
        return []

    parsed = feedparser.parse(resp.content)
    if parsed.bozo and not parsed.entries:
        print(f"[RSS] Flux illisible '{feed['name']}' ({feed['url']}): "
              f"{parsed.bozo_exception}", file=sys.stderr)
        return []

    scope = feed.get("scope", "regional_generalist")
    items = []
    for entry in parsed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        haystack = f"{title} {summary}".lower()

        has_event = any(kw in haystack for kw in RSS_KEYWORDS)
        has_sector = any(kw.lower() in haystack for kw in SECTOR_KEYWORDS)
        has_region = any(kw in haystack for kw in REGION_KEYWORDS)

        if scope == "sector_national":
            if not has_region:
                continue
        else:  # regional_generalist
            if not (has_event and has_sector):
                continue

        pub_dt = None
        if entry.get("published_parsed"):
            pub_dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        if pub_dt and pub_dt < since:
            continue

        items.append({
            "source": feed["name"],
            "zone": feed["zone"],
            "titre": title,
            "date": entry.get("published", ""),
            "url": entry.get("link", ""),
            "signal_fort": has_event,
        })
    return items


def collect_rss(since: datetime) -> list[dict[str, Any]]:
    results = []
    for feed in RSS_FEEDS:
        results.extend(parse_rss(feed, since))
    return results


# --------------------------------------------------------------------------
# Digest
# --------------------------------------------------------------------------

def build_text_digest(bodacc_items: list[dict[str, Any]],
                       rss_items: list[dict[str, Any]],
                       date_from: str, date_to: str) -> str:
    lines = [
        f"VEILLE AGROALIMENTAIRE - 84/30/13 - du {date_from} au {date_to}",
        "=" * 70, "",
        f"BODACC ({len(bodacc_items)} avis)", "-" * 70,
    ]
    if not bodacc_items:
        lines.append("Aucun avis BODACC agroalimentaire sur la période.")
    for item in bodacc_items:
        lines.append(
            f"- [{item['famille']}] {item['entreprise']} ({item['ville']}, "
            f"{item['numerodepartement']}) - {item['date_parution']}"
        )
        lines.append(f"    {item['url']}")
    lines += ["", f"PRESSE ({len(rss_items)} articles)", "-" * 70]
    if not rss_items:
        lines.append("Aucun article agroalimentaire pertinent sur la période.")
    for item in rss_items:
        tag = " [SIGNAL FORT]" if item.get("signal_fort") else ""
        lines.append(
            f"- [{item['source']}]{tag} {item['titre']} - {item['date']}"
        )
        lines.append(f"    {item['url']}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days", type=int, default=LOOKBACK_DAYS_DEFAULT,
        help=f"Fenêtre de recherche en jours (défaut: {LOOKBACK_DAYS_DEFAULT})",
    )
    parser.add_argument("--out-json", help="Chemin du fichier JSON de sortie")
    parser.add_argument("--out-text", help="Chemin du fichier digest texte")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=args.lookback_days)
    date_from = since.strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    bodacc_items = collect_bodacc(date_from, date_to)
    rss_items = collect_rss(since)

    payload = {
        "generated_at": now.isoformat(),
        "window": {"from": date_from, "to": date_to,
                    "lookback_days": args.lookback_days},
        "bodacc": bodacc_items,
        "presse": rss_items,
    }

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))

    digest = build_text_digest(bodacc_items, rss_items, date_from, date_to)
    if args.out_text:
        with open(args.out_text, "w", encoding="utf-8") as fh:
            fh.write(digest)
    else:
        print("\n" + digest, file=sys.stderr)


if __name__ == "__main__":
    main()
