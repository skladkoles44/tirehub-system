from functools import lru_cache
import hashlib

@lru_cache(maxsize=256)
def compute_layout_fingerprint(role_key):
    signature="|".join(role_key)+f"|colcount_{len(role_key)}"
    return hashlib.sha1(signature.encode()).hexdigest()[:10]
