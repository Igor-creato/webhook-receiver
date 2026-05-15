"""Universal exactly-once transaction-identity resolver.

BYTE-IDENTICAL Python mirror of the PHP
`Cashback_Api_Client::resolve_uniq_id()` in the cash-back plugin
(includes/class-cashback-api-client.php). Any divergence between the two
implementations means silent duplicate/loss, so the contract is pinned by the
shared fixture development/test/fixtures/dedup-vectors.json — both the PHP
PHPUnit test and the Python replay run the exact same vectors.

Resolution order:
  1. Native id — if the value mapped to ``uniq_id`` is non-empty and the
     network is not flagged ``has_native_action_id: false`` -> return it
     verbatim. Preserves Admitad/Advcake/EPN behaviour and guarantees
     webhook==XML parity (both sides map the same logical source).
  2. Synthetic — only when native is empty AND ``has_native_action_id`` is
     exactly ``False``:
     ``syn_`` + sha1( lower(slug) | <synthetic_fields in order> [ | click_id ] ).
     Stable fields only (NOT status/amount/date) so a status-change
     re-postback resolves to the SAME id.
  3. No identity -> ("", "no_dedup_inputs"); caller routes to DLQ, never
     inserts.
"""

from __future__ import annotations

import hashlib
from typing import Any

# Exact PHP trim() default character set: space, \t, \n, \r, NUL, vertical tab.
# Matching this byte-for-byte is what keeps the resolver parity-safe.
_PHP_TRIM = " \t\n\r\x00\x0b"


def _s(value: Any) -> str:
    """Coerce like PHP (string): None -> '' (NOT 'None'), else str()."""
    if value is None:
        return ""
    return str(value)


def _trim(value: Any) -> str:
    return _s(value).strip(_PHP_TRIM)


def resolve_uniq_id(
    slug: str,
    native_uniq_id: str,
    fields: dict[str, Any],
    dedup_identity: dict[str, Any] | None,
) -> tuple[str, str | None]:
    """Return (uniq_id, reason|None). See module docstring for the contract.

    :param slug: network slug (canonicalised lower).
    :param native_uniq_id: value mapped to the ``uniq_id`` column.
    :param fields: canonical fields for the synthetic branch
        ({'order_number','offer_id','action_type','click_id'}).
    :param dedup_identity: the network's identity contract
        (cashback_affiliate_networks.dedup_identity); None == legacy
        has_native_action_id:true.
    """
    native = _trim(native_uniq_id)

    # null contract == legacy: assume a native id exists (pre-v16 behaviour).
    if dedup_identity is None:
        has_native = True
    else:
        has_native = dedup_identity.get("has_native_action_id", True) is not False

    if native != "":
        return native, None

    if has_native is True:
        # Native id was expected but absent — nothing to dedup on -> DLQ.
        return "", "no_dedup_inputs"

    # --- Synthetic branch (has_native_action_id === false) ---
    synthetic_fields = None
    if dedup_identity is not None:
        synthetic_fields = dedup_identity.get("synthetic_fields")
    if not isinstance(synthetic_fields, list) or synthetic_fields == []:
        synthetic_fields = ["order_number", "offer_id", "action_type"]

    include_click = False
    if dedup_identity is not None:
        include_click = dedup_identity.get("synthetic_include_click_id", False) is True

    components: list[str] = []
    all_empty = True
    for fname in synthetic_fields:
        val = _trim(fields.get(str(fname), ""))
        if val != "":
            all_empty = False
        components.append(val)
    if include_click:
        val = _trim(fields.get("click_id", ""))
        if val != "":
            all_empty = False
        components.append(val)

    if all_empty:
        return "", "no_dedup_inputs"

    components.insert(0, _trim(slug).lower())

    digest = hashlib.sha1("|".join(components).encode("utf-8")).hexdigest()
    return "syn_" + digest, None
