"""Split SteamSpy's comma-separated developer and publisher strings.

SteamSpy joins several companies with ", ", but company names can contain
commas themselves ("Valve, Inc.", "Bandai Namco Entertainment Co., Ltd.").
The rule below is a heuristic, documented in docs/sources.md: a part that is
only a legal-form suffix is joined back onto the name before it.
"""

import html
import re


MAX_NAME_LENGTH = 255

# Legal-form suffixes, compared with dots and spaces removed, lower case.
LEGAL_SUFFIXES = {
    "ab", "ag", "as", "bv", "co", "coltd", "corp", "corporation", "gmbh",
    "inc", "incorporated", "kk", "limited", "llc", "llp", "lp", "ltd",
    "ltda", "nv", "oy", "plc", "pty", "ptyltd", "sa", "sas", "sl", "spa",
    "srl", "sro",
}


def is_legal_suffix(part):
    return re.sub(r"[.\s]", "", part.lower()) in LEGAL_SUFFIXES


def split_company_names(raw):
    """Return the distinct company names in a SteamSpy string, in order."""
    if raw is None:
        return []

    text = html.unescape(str(raw)).strip()

    if not text:
        return []

    names = []

    for part in (piece.strip() for piece in text.split(",")):
        if not part:
            continue

        if names and is_legal_suffix(part):
            names[-1] = f"{names[-1]}, {part}"
        else:
            names.append(part)

    distinct = []
    seen = set()

    for name in names:
        name = re.sub(r"\s+", " ", name)[:MAX_NAME_LENGTH]
        key = name.casefold()

        if key not in seen:
            seen.add(key)
            distinct.append(name)

    return distinct
