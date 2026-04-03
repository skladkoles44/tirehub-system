#!/usr/bin/env python3
"""
column_classifier_v2
- root-based classification
- normalization
- scoring + priority
- no supplier-specific rules
"""

import re

# ==================== NORMALIZE ====================

def norm(h: str) -> str:
    if not h:
        return ""
    h = h.lower()
    h = re.sub(r"[()\-_/]+", " ", h)
    h = re.sub(r"\s+", " ", h)
    return h.strip()


# ==================== ROOTS ====================

ROOTS = {
    "price_wholesale": ["опт", "b2b", "wholesale"],
    "price_retail": ["розн", "retail"],
    "stock": ["остат", "налич", "qty", "stock"],
    "sku": ["артикул", "sku", "код"],
    "model": ["наимен", "товар", "модель"],
    "brand": ["бренд", "производ", "марка"],
    "size": ["размер", "диаметр", "ширин", "профил"],
    "season": ["сезон", "летн", "зим", "всесез"],
    "country": ["стран"],
}


# ==================== PRIORITY ====================

PRIORITY = {
    "price_wholesale": 100,
    "price_retail": 95,
    "stock": 90,
    "sku": 80,
    "model": 70,
    "brand": 60,
    "size": 50,
    "season": 40,
    "country": 30,
}


# ==================== SCORING ====================

def _score_role(h_norm: str):
    scores = {k: 0 for k in ROOTS}

    for role, words in ROOTS.items():
        for w in words:
            if w in h_norm:
                scores[role] += 1

    # --- CONTEXT BOOST ---
    if "цена" in h_norm or "price" in h_norm:
        if "опт" in h_norm or "b2b" in h_norm:
            scores["price_wholesale"] += 2
        if "розн" in h_norm:
            scores["price_retail"] += 2

    # --- SELECT BEST ---
    best_role = None
    best_score = 0

    for role, sc in scores.items():
        if sc > 0:
            weighted = sc * 10 + PRIORITY.get(role, 0)
            if weighted > best_score:
                best_score = weighted
                best_role = role

    return best_role


# ==================== PUBLIC API ====================

def classify_columns(headers, samples=None):
    roles = []

    for h in headers:
        h_norm = norm(h)
        role = _score_role(h_norm)
        roles.append(role)

    return roles
