"""
Matching a feed name against a player we already hold, without touching the database.

Genius gives the same player two spellings: the box score has the full name
("Dzana Crnac") and the play-by-play an abbreviation ("D. Crnac"). Both reach
get_or_create_player, so without a match every player ends up with two records
and anything grouped by player id (lineups, plus-minus) counts them twice.
"""
import unicodedata

# Letters that don't decompose into a base letter plus an accent.
_LETTER_FOLDS = str.maketrans({
    "þ": "th", "ð": "d", "æ": "ae", "œ": "oe", "ø": "o", "ł": "l", "đ": "d", "ß": "ss", "ı": "i",
})


def fold_name_part(part: str) -> str:
    """Lower-case, accent-free spelling: "Þóranna" -> "thoranna", "Magnúsdóttir" -> "magnusdottir"."""
    folded = unicodedata.normalize("NFKD", (part or "").lower().translate(_LETTER_FOLDS))
    return "".join(c for c in folded if not unicodedata.combining(c))


def _first_and_last(name: str):
    parts = (name or "").split()
    if len(parts) < 2:
        return None
    # "D." is an initial like "D": drop the full stop so its length is 1.
    return fold_name_part(parts[0]).replace(".", ""), fold_name_part(parts[-1]).replace(".", "")


def initial_matches_full_name(a: str, b: str, shirt_a=None, shirt_b=None) -> bool:
    """
    True when one name is the other abbreviated to a first initial ("D. Crnac" and
    "Dzana Crnac", "T. Hodge-Carr" and "Þóranna Kika Hodge-Carr"). Two different shirt
    numbers mean two different people, so those never match.
    """
    fa, fb = _first_and_last(a), _first_and_last(b)
    if not fa or not fb:
        return False
    (first_a, last_a), (first_b, last_b) = fa, fb
    if not first_a or not first_b or last_a != last_b:
        return False
    if shirt_a is not None and shirt_b is not None and str(shirt_a) != str(shirt_b):
        return False
    return (len(first_a) == 1 and first_b.startswith(first_a)) or (len(first_b) == 1 and first_a.startswith(first_b))
