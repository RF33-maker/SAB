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


# Byte sequences UTF-8 text picks up when it is decoded as Windows-1252 somewhere
# upstream: "La’Quarne" arrives as "Laâ€™Quarne", "José" as "JosÃ©".
_MOJIBAKE_MARKERS = ("â€", "Ã", "Â")


def clean_feed_name(name: str) -> str:
    """
    The name to store and match on: mangled encoding repaired and runs of spaces
    collapsed ("Dylan " + "Ameen" became "Dylan  Ameen", which no exact match found).
    """
    cleaned = " ".join((name or "").split())
    if any(marker in cleaned for marker in _MOJIBAKE_MARKERS):
        try:
            cleaned = cleaned.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return cleaned


def name_key(name: str) -> str:
    """Spelling-insensitive form for exact comparison: "D. AMEEN" and "d ameen" are the same key."""
    return " ".join(fold_name_part(part).replace(".", "") for part in clean_feed_name(name).split())


def is_initial_name(name: str) -> bool:
    """True for play-by-play abbreviations like "S. Walker", whose first word is a single letter."""
    parts = clean_feed_name(name).split()
    return len(parts) >= 2 and len(parts[0].replace(".", "")) <= 1


def _shirt_fits(feed_shirt, shirts) -> bool:
    # Players change numbers between games and seasons, so a candidate fits when the
    # feed's number is any number they have worn. No number on either side proves nothing.
    if feed_shirt is None or not shirts:
        return True
    return str(feed_shirt) in shirts


def pick_player(feed_name, feed_shirt, candidates, league_id=None, shirts_worn=None, similarity_threshold=0.85):
    """
    Choose which existing player (if any) a feed name belongs to, from the players on
    the same team. Returns (candidate, how) or (None, None) to create a new record.

    candidates: dicts with id, full_name, shirtNumber, aliases, league_id, created_at.
    shirts_worn: callable(ids) -> {id: set of shirt numbers as strings}, consulted only
    when an initial-only match needs a number to tell two people apart.

    Order matters, and the answer must not depend on the order the database returns
    rows in: re-polling a live game has to land every player on the same id it did last
    time, or the game gets a second stat row under the new id.
      1. the same name, ignoring case, spacing, accents and full stops
      2. a name the player is known by (players.aliases)
      3. a close spelling of a full name ("Mcdonald" / "McDonald")
      4. one name is the other's first initial ("A. Smith" / "Alexander Smith")
    A full name goes to the player's full-name record before any "X. Surname" stub
    the play-by-play left behind (17 Oct: box scores were landing on those stubs).
    """
    from difflib import SequenceMatcher

    feed_name = clean_feed_name(feed_name)
    wanted = name_key(feed_name)
    if not wanted or not candidates:
        return None, None

    def shirts_of(c):
        return {str(c["shirtNumber"])} if c.get("shirtNumber") is not None else set()

    def rank(c, extra_shirts=None):
        shirts = shirts_of(c) | (extra_shirts or set())
        return (
            0 if feed_shirt is not None and str(feed_shirt) in shirts else 1,
            0 if league_id and c.get("league_id") == league_id else 1,
            1 if is_initial_name(c.get("full_name")) else 0,
            str(c.get("created_at") or ""),
            str(c.get("id")),
        )

    exact = [c for c in candidates if name_key(c.get("full_name")) == wanted]
    if exact:
        return min(exact, key=rank), "exact"

    aliased = [c for c in candidates if any(name_key(a) == wanted for a in (c.get("aliases") or []) if a)]
    if aliased:
        return min(aliased, key=rank), "alias"

    if not is_initial_name(feed_name):
        best, best_score = None, 0.0
        for c in sorted(candidates, key=rank):
            if is_initial_name(c.get("full_name")):
                continue
            score = SequenceMatcher(None, wanted, name_key(c.get("full_name"))).ratio()
            if score >= similarity_threshold and score > best_score:
                best, best_score = c, score
        if best:
            return best, "fuzzy"

    initial = [c for c in candidates if initial_matches_full_name(feed_name, clean_feed_name(c.get("full_name")))]
    if not initial:
        return None, None
    worn = shirts_worn([c["id"] for c in initial]) if shirts_worn else {}

    def all_shirts(c):
        return shirts_of(c) | worn.get(c["id"], set())

    fitting = [c for c in initial if _shirt_fits(feed_shirt, all_shirts(c))]
    # "A. Smith" could be any of several teammates (brothers, namesakes): with more than
    # one different name to choose from, only the one whose shirt confirms it will do.
    if len({name_key(c.get("full_name")) for c in fitting}) > 1:
        fitting = [c for c in fitting if feed_shirt is not None and str(feed_shirt) in all_shirts(c)]
        if len({name_key(c.get("full_name")) for c in fitting}) != 1:
            return None, None
    if not fitting:
        return None, None
    return min(fitting, key=lambda c: rank(c, worn.get(c["id"], set()))), "initial"
