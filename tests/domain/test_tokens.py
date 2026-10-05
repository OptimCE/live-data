"""Enrolment tokens.

The cases that matter are the ones a support call produces: a code read aloud, a
code typed without hyphens, a code where someone saw a 1 and typed an l.
"""

import re

import pytest

from domain.tokens import (
    TOKEN_LENGTH,
    format_token,
    generate,
    hash_supplied,
    hash_token,
    normalise,
)
from shared.const import CROCKFORD_ALPHABET, TOKEN_ENTROPY_BITS


class TestGeneration:
    def test_a_token_round_trips_through_normalisation(self):
        token = generate()
        normalised = normalise(token)
        assert normalised is not None
        assert len(normalised) == TOKEN_LENGTH

    def test_it_carries_at_least_the_required_entropy(self):
        """plan 8.3: at least 128 bits.

        Asserted on the LENGTH rather than by sampling, because sampling a
        random generator proves nothing at test scale.
        """
        assert TOKEN_LENGTH * 5 >= TOKEN_ENTROPY_BITS

    def test_it_uses_only_the_crockford_alphabet(self):
        """No I, L, O or U - the four characters that make a code unreadable
        aloud or accidentally obscene."""
        for _ in range(50):
            raw = normalise(generate())
            assert raw is not None
            assert set(raw) <= set(CROCKFORD_ALPHABET)
        assert not (set("ILOU") & set(CROCKFORD_ALPHABET))

    def test_tokens_are_distinct(self):
        """A weak smoke test for the obvious catastrophe - a constant token -
        not a statistical claim about the generator."""
        assert len({generate() for _ in range(200)}) == 200

    def test_it_is_grouped_for_a_human(self):
        token = generate()
        assert "-" in token
        assert re.fullmatch(r"[0-9A-Z]+(-[0-9A-Z]+)+", token)


class TestNormalisationIsThePointOfTheAlphabet:
    def test_hyphens_are_presentation_only(self):
        token = generate()
        assert normalise(token) == normalise(token.replace("-", ""))

    def test_spaces_are_tolerated(self):
        """Someone reading a code aloud pauses; someone typing it uses spaces."""
        token = generate()
        assert normalise(token.replace("-", " ")) == normalise(token)

    def test_case_is_folded(self):
        token = generate()
        assert normalise(token.lower()) == normalise(token)

    @pytest.mark.parametrize(("typed", "meant"), [("I", "1"), ("L", "1"), ("O", "0")])
    def test_the_read_aloud_confusions_decode(self, typed: str, meant: str):
        """Crockford's actual decoding rule, and the reason to use it.

        Omitting I/L/O from the alphabet helps whoever GENERATES the code.
        Decoding them anyway is what helps whoever TYPES it: a person told
        "oh" types O, and a person reading a serif 1 types l.
        """
        body = CROCKFORD_ALPHABET[0] * (TOKEN_LENGTH - 1)
        assert normalise(body + typed) == body + meant

    def test_a_lowercase_l_is_a_one(self):
        """The specific case that generates support calls."""
        body = "7" * (TOKEN_LENGTH - 1)
        assert normalise(body + "l") == body + "1"


class TestRejection:
    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "TOO-SHORT",
            "U" * 26,  # U is not in the alphabet at all
            "!" * 26,
        ],
    )
    def test_a_malformed_token_is_none_not_an_exception(self, bad: str):
        """None, not a raise.

        The caller answers a malformed token exactly as it answers an unknown
        one - a single opaque error. Distinguishing them would tell an attacker
        when they have the SHAPE right, which is the feedback a search needs.
        """
        assert normalise(bad) is None
        assert hash_supplied(bad) is None

    def test_one_character_too_many_is_rejected(self):
        assert normalise(generate() + "7") is None

    def test_one_character_too_few_is_rejected(self):
        token = normalise(generate())
        assert token is not None
        assert normalise(token[:-1]) is None


class TestHashing:
    def test_the_hash_is_64_hex_characters(self):
        """CHAR(64) in the schema - a mismatch would be silently truncated."""
        digest = hash_supplied(generate())
        assert digest is not None
        assert len(digest) == 64
        assert re.fullmatch(r"[0-9a-f]{64}", digest)

    def test_equivalent_spellings_hash_identically(self):
        """The whole point: hashing what the user typed, rather than the
        normalised form, would make `k7m9-p2qr` and `K7M9P2QR` different
        tokens."""
        token = generate()
        assert (
            hash_supplied(token)
            == hash_supplied(token.lower())
            == hash_supplied(token.replace("-", ""))
            == hash_supplied(token.replace("-", " ").lower())
        )

    def test_different_tokens_hash_differently(self):
        assert hash_supplied(generate()) != hash_supplied(generate())

    def test_hash_token_takes_the_normalised_form(self):
        token = generate()
        normalised = normalise(token)
        assert normalised is not None
        assert hash_token(normalised) == hash_supplied(token)


class TestFormatting:
    def test_formatting_and_normalising_are_inverses(self):
        raw = normalise(generate())
        assert raw is not None
        assert normalise(format_token(raw)) == raw
