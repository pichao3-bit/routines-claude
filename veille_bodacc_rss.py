#!/usr/bin/env python3
"""
veille_bodacc_rss.py

Veille juridique/économique locale (Vaucluse 84, Gard 30, Bouches-du-Rhône 13)
pour repérer tôt les procédures collectives, ventes/cessions de fonds, et
changements de dirigeant, en combinant :
  1. L'API ouverte BODACC (DILA / data.gouv.fr) - gratuite, sans clé.
  2. Les flux RSS de la presse économique régionale.

Toutes les URLs et tous les noms de champs ci-dessous ont été vérifiés en
direct (requêtes réelles) le 2026-07-01. Voir README.md pour le détail de ce
qui a été testé et ce qui reste à surveiller.

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

# Mots-clés pour filtrer les flux RSS de presse généraliste (ils ne sont pas
# limités à l'économie/justice, sauf quand une catégorie dédiée existe).
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
RSS_FEEDS = [
    {
        "name": "L'Echo du Mardi - Economie",
        "url": "https://www.echodumardi.com/category/economie/feed",
        "zone": "84",
    },
    {
        "name": "Objectif Gard - Economie",
        "url": "https://www.objectifgard.com/economie/feed",
        "zone": "30",
    },
    {
        "name": "Le Journal des Entreprises - Occitanie",
        "url": "https://www.lejournaldesentreprises.com/rss-occitanie",
        "zone": "30",
    },
    {
        "name": "Le Journal des Entreprises - Région Sud",
        "url": "https://www.lejournaldesentreprises.com/rss-region-sud",
        "zone": "84/13",
    },
]

REQUEST_TIMEOUT = 20
USER_AGENT = "veille-transition-bot/1.0 (+contact: pichao3@gmail.com)"


# --------------------------------------------------------------------------
# BODACC
# --------------------------------------------------------------------------

def fetch_bodacc(dept: str, famille: str, date_from: str, date_to: str,
                  rows: int = 50) -> list[dict[str, Any]]:
    """Interroge l'API BODACC pour un département et une famille d'avis
    donnés, sur une fenêtre de dates [date_from, date_to] (YYYY-MM-DD).
    Retourne la liste des 'fields' de chaque enregistrement.
    """
    params = {
        "dataset": BODACC_DATASET,
        "q": f"dateparution:[{date_from} TO {date_to}]",
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
    et dont le titre/résumé contient un des RSS_KEYWORDS."""
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

    items = []
    for entry in parsed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        haystack = f"{title} {summary}".lower()
        if not any(kw in haystack for kw in RSS_KEYWORDS):
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
        f"VEILLE JURIDIQUE/ECONOMIQUE - 84/30/13 - du {date_from} au {date_to}",
        "=" * 70, "",
        f"BODACC ({len(bodacc_items)} avis)", "-" * 70,
    ]
    if not bodacc_items:
        lines.append("Aucun avis BODACC sur la période.")
    for item in bodacc_items:
        lines.append(
            f"- [{item['famille']}] {item['entreprise']} ({item['ville']}, "
            f"{item['numerodepartement']}) - {item['date_parution']}"
        )
        lines.append(f"    {item['url']}")
    lines += ["", f"PRESSE REGIONALE ({len(rss_items)} articles)", "-" * 70]
    if not rss_items:
        lines.append("Aucun article correspondant aux mots-clés sur la période.")
    for item in rss_items:
        lines.append(f"- [{item['source']}] {item['titre']} - {item['date']}")
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
