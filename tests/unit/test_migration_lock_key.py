"""The migration lock's key must be stable, and stable across servers.

The lock is what stops two concurrent bootstraps destroying each other's work,
and it only does that if both runs compute the *same* key. A key that varied
between a PostgreSQL 14 source and a 17 one — which is exactly what the
server's own ``hashtext()`` does, being an implementation detail — would let
both runs take "the" lock and proceed. So the key is derived in Python, and
that derivation is pinned here.
"""

from __future__ import annotations

from pg_emigrant.guards import _lock_key

INT32_MIN, INT32_MAX = -(2 ** 31), 2 ** 31 - 1


def test_the_key_is_deterministic():
    assert _lock_key("pg_emigrant_sub_myapp") == _lock_key("pg_emigrant_sub_myapp")


def test_both_halves_fit_the_int4_parameters_they_are_passed_as():
    """``pg_try_advisory_lock(int, int)`` rejects anything outside int4, and it
    would do so at the worst moment — inside the guard, turning a refusal into
    a generic failure whose rollback drops the other run's slot."""
    for name in ("a", "pg_emigrant_sub_myapp", "x" * 200, "Ünïcödé", "sub_ワーク"):
        hi, lo = _lock_key(name)
        assert INT32_MIN <= hi <= INT32_MAX, (name, hi)
        assert INT32_MIN <= lo <= INT32_MAX, (name, lo)


def test_different_slots_get_different_keys():
    """Two databases migrated at once must not block each other: their slot
    names are already distinct by construction, and the keys have to follow."""
    names = ["pg_emigrant_sub_a", "pg_emigrant_sub_b", "pg_emigrant_sub_a_",
             "pg_emigrant_sub_A", "other_sub_a"]
    keys = {_lock_key(n) for n in names}
    assert len(keys) == len(names), keys


def test_the_key_is_namespaced_to_this_tool():
    """The advisory-lock space is shared with whatever else uses the database.
    Hashing the bare slot name would collide with any application that hashes
    the same string — so the derivation carries a prefix."""
    import hashlib

    hi, lo = _lock_key("myslot")
    bare = hashlib.sha256(b"myslot").digest()
    assert (int.from_bytes(bare[0:4], "big", signed=True),
            int.from_bytes(bare[4:8], "big", signed=True)) != (hi, lo)
