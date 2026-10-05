"""Enrolment tokens: generate, format, normalise, hash. Pure - no I/O, no clock.

plan 8.3. A token is the ONLY credential on the public enrolment leg, and it is
read aloud down a telephone, typed into a captive portal by someone holding a
phone in a basement, or scanned from a QR code.

----------------------------------------------------------------------------
WHY CROCKFORD BASE32, AND WHY NORMALISATION IS THE POINT OF IT

The alphabet omits I, L, O and U. The first three because they are
indistinguishable from 1, 1 and 0 in most fonts and in speech; U because
excluding it makes an accidental obscenity much less likely in a code a support
agent has to read out.

But omitting them from the ALPHABET only helps the person generating. What helps
the person TYPING is decoding them anyway: someone told "oh-scarlet" will type
`O`, and someone reading a serif `1` will type `l`. So `normalise()` maps
I, L -> 1 and O -> 0 and folds case, which is Crockford's actual specification
and the reason to use it over plain base32.

----------------------------------------------------------------------------
WHY sha256 AND NOT A KDF

The token is 128 bits of CSPRNG output. There is no low-entropy secret here for
a work factor to protect - an attacker gains nothing from a slow hash against a
uniformly random 128-bit value, and everything from the fact that guessing is
already infeasible.

A KDF would cost two things that matter:

  * ~100 ms inside a request KrakenD cuts at 3000 ms, on the one endpoint that
    is unauthenticated and therefore the one an attacker can drive; and
  * the single-index lookup. A bcrypt hash cannot be looked up by value, so the
    server would have to SCAN every live token and compare each - turning an
    O(1) index probe into O(n) work per guess, which is a denial of service the
    KDF itself created.

plan 8.3 says the token is "compared in constant time". That is satisfied HERE
BY CONSTRUCTION rather than by a byte loop: the comparison is a b-tree index
probe on a uniformly distributed 256-bit key, which reveals nothing about a
near-miss because there is no such thing as a near-miss in a hash space. Written
down because the plan's phrasing invites someone to add `hmac.compare_digest`
around a scan, which would be slower AND worse.

The only meaningful application-level signal is a GLOBAL "token not found"
counter: a per-row attempt counter is useless, because a guess never finds a row
to count against.
"""

import hashlib
import secrets
from typing import Final

from shared.const import CROCKFORD_ALPHABET, TOKEN_ENTROPY_BITS, TOKEN_GROUP_SIZE

# 128 bits / 5 bits per character, rounded up. 26 characters carry 130 bits of
# space, of which we fill 128 - the top two bits are always zero, which is
# harmless and keeps the length fixed so a malformed code is rejected on length
# before anything else looks at it.
TOKEN_LENGTH: Final[int] = -(-TOKEN_ENTROPY_BITS // 5)  # ceil, without math

# Crockford's decoding rules, applied on the way IN. See the module docstring:
# the alphabet protects the writer, this table protects the typist.
_DECODE_ALIASES: Final[dict[str, str]] = {
    "I": "1",
    "L": "1",
    "O": "0",
}

_ALPHABET_INDEX: Final[dict[str, int]] = {c: i for i, c in enumerate(CROCKFORD_ALPHABET)}

# Everything a human puts between groups. Built from chr() rather than
# written as literals, because two of these are invisible in a diff and one
# of them - U+00A0, the non-breaking space - is what a token pasted out of a
# web page or a PDF carries. A member has no way to see why their perfectly
# correct code is being refused, so it is stripped rather than rejected.
_SEPARATORS: Final[frozenset[str]] = frozenset(
    {
        "-",
        chr(0x20),  # space
        chr(0x09),  # tab
        chr(0xA0),  # no-break space
    }
)


def generate() -> str:
    """A fresh token, formatted for a human.

    `secrets.randbits` and not `random`: this is a credential. ruff's bandit
    rules would catch `random` here, but the reason matters more than the lint.
    """
    value = secrets.randbits(TOKEN_ENTROPY_BITS)
    return format_token(_encode(value))


def _encode(value: int) -> str:
    """Fixed-width Crockford base32, most significant character first."""
    chars = []
    for _ in range(TOKEN_LENGTH):
        value, remainder = divmod(value, 32)
        chars.append(CROCKFORD_ALPHABET[remainder])
    return "".join(reversed(chars))


def format_token(raw: str) -> str:
    """Group with hyphens, for reading aloud and for typing.

    The hyphens are presentation only - `normalise()` strips them - so a member
    who types the code without them, or with spaces instead, still enrols.
    """
    return "-".join(raw[i : i + TOKEN_GROUP_SIZE] for i in range(0, len(raw), TOKEN_GROUP_SIZE))


def normalise(supplied: str) -> str | None:
    """Canonicalise a token as typed. None when it cannot be one.

    Returning None rather than raising, because the caller's response to a
    malformed token is identical to its response to an unknown one - a single
    opaque error. Distinguishing them would turn this endpoint into an oracle
    that tells an attacker when they have the SHAPE right, which is exactly the
    feedback a brute-force search needs.
    """
    if not supplied:
        return None
    stripped = "".join(ch for ch in supplied.upper() if ch not in _SEPARATORS)
    decoded = "".join(_DECODE_ALIASES.get(ch, ch) for ch in stripped)
    if len(decoded) != TOKEN_LENGTH:
        return None
    if any(ch not in _ALPHABET_INDEX for ch in decoded):
        return None
    return decoded


def hash_token(normalised: str) -> str:
    """SHA-256 hex of a NORMALISED token. 64 characters, matching CHAR(64).

    Takes the normalised form on purpose: hashing what the user typed would make
    `k7m9-p2qr` and `K7M9P2QR` different tokens, and the whole point of the
    alphabet is that they are the same one.
    """
    return hashlib.sha256(normalised.encode("ascii")).hexdigest()


def hash_supplied(supplied: str) -> str | None:
    """Normalise then hash, in one step. None when the input cannot be a token.

    The lookup key for the enrolment endpoint.
    """
    normalised = normalise(supplied)
    if normalised is None:
        return None
    return hash_token(normalised)
