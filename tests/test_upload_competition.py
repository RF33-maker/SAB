"""
Which competition a PDF upload goes into. League admins list the PDF titles a
competition accepts; REBA SL runs one competition per age group per stop while
the PDFs carry one title per stop, so the team names' age group picks between
them.
"""
import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "test")

from app.utils import pdf_parser
from app.utils.pdf_parser import _age_group_of, _resolve_upload_competition


class _Query:
    def __init__(self, db, table):
        self.db, self.table, self.filters, self.row, self.n = db, table, [], None, None

    def select(self, _cols):
        return self

    def eq(self, col, val):
        self.filters.append(lambda r: r.get(col) == val)
        return self

    def in_(self, col, vals):
        vals = set(vals)
        self.filters.append(lambda r: r.get(col) in vals)
        return self

    def ilike(self, col, pattern):
        plain = pattern.replace("\\_", "_").replace("\\%", "%").replace("\\\\", "\\").lower()
        self.filters.append(lambda r: (r.get(col) or "").lower() == plain)
        return self

    def limit(self, n):
        self.n = n
        return self

    def insert(self, row):
        self.row = dict(row)
        return self

    def execute(self):
        rows = self.db.tables.setdefault(self.table, [])
        if self.row is not None:
            if self.table == "competitions":
                self.row.setdefault("league_id", str(uuid.uuid4()))
            rows.append(self.row)
            return type("R", (), {"data": [self.row]})
        out = [r for r in rows if all(f(r) for f in self.filters)]
        return type("R", (), {"data": out[: self.n] if self.n else out})


class FakeDB:
    def __init__(self, competitions=(), upload_names=()):
        self.tables = {"competitions": [dict(c) for c in competitions], "competition_upload_names": [dict(u) for u in upload_names]}

    def table(self, name):
        return _Query(self, name)


SEASON = {"league_id": "s2", "name": "REBA SL Season 2", "season": "Season 2", "competition_id": "brand", "is_public": True}
B15 = {"league_id": "b15", "name": "REBA SL 15U Bristol stop FIBA", "parent_league_id": "s2", "age_group": "15U", "stop": 2}
B18 = {"league_id": "b18", "name": "REBA SL 18U Bristol stop FIBA", "parent_league_id": "s2", "age_group": "18U", "stop": 2}
BRISTOL_TITLE = [
    {"league_id": "b15", "name": "REBA SL Bristol stop FIBA"},
    {"league_id": "b18", "name": "REBA SL Bristol stop FIBA"},
]


def resolve(db, title, selected, teams):
    return _resolve_upload_competition(db, title, selected, teams, "user-1")


def test_age_group_from_team_names():
    assert _age_group_of(["Team Black 15U", "Team Gold 15U"]) == "15U"
    assert _age_group_of(["Lions U18 Men", "Hawks U18 Men"]) == "U18"
    assert _age_group_of(["Team 16+ Red", "Team 16+ Blue"]) == "16+"
    assert _age_group_of(["Team Black 15U", "Team Gold 18U"]) is None
    assert _age_group_of(["Team 3", "Team 4"]) is None


def test_shared_title_goes_to_the_teams_age_group():
    db = FakeDB([SEASON, B15, B18], BRISTOL_TITLE)
    assert resolve(db, "REBA SL Bristol stop FIBA", "s2", ["Team Black 15U", "Team Gold 15U"]) == {"league_id": "b15", "created": False}
    assert resolve(db, "reba sl bristol stop fiba", "s2", ["Team Black 18U", "Team Gold 18U"]) == {"league_id": "b18", "created": False}


def test_listed_title_found_with_nothing_picked():
    db = FakeDB([SEASON, B15, B18], BRISTOL_TITLE)
    assert resolve(db, "REBA SL Bristol stop FIBA", None, ["Team Black 18U", "Team Gold 18U"])["league_id"] == "b18"


def test_new_stop_under_a_season_gets_one_competition_per_age_group():
    db = FakeDB([SEASON, B15, B18], BRISTOL_TITLE)
    first = resolve(db, "REBA SL Leeds stop FIBA", "s2", ["Team Black 15U", "Team Gold 15U"])
    assert first["created"] is True
    leeds15 = next(c for c in db.tables["competitions"] if c["league_id"] == first["league_id"])
    assert leeds15["name"] == "REBA SL Leeds stop FIBA (15U)"
    assert (leeds15["parent_league_id"], leeds15["season"], leeds15["competition_id"], leeds15["age_group"]) == ("s2", "Season 2", "brand", "15U")
    assert leeds15["created_by"] == "user-1"

    second = resolve(db, "REBA SL Leeds stop FIBA", "s2", ["Team Black 18U", "Team Gold 18U"])
    assert second["created"] is True and second["league_id"] != first["league_id"]

    again = resolve(db, "REBA SL Leeds stop FIBA", "s2", ["Team Red 15U", "Team Blue 15U"])
    assert again == {"league_id": first["league_id"], "created": False}


def test_a_competition_with_nothing_under_it_takes_the_upload():
    leaf = {"league_id": "weabl", "name": "WEABL 2025-26"}
    db = FakeDB([leaf])
    assert resolve(db, "WEABL 2025-26 Round 12", "weabl", ["A", "B"]) == {"league_id": "weabl", "created": False}


def test_a_league_with_only_a_finals_child_is_not_split():
    d2 = {"league_id": "d2", "name": "Ball Park Summer League D2 FIBA"}
    finals = {"league_id": "d2f", "name": "Ball Park Summer League D2 Finals FIBA", "parent_league_id": "d2"}
    db = FakeDB([d2, finals])
    assert resolve(db, "Ball Park Summer League Division 2 FIBA", "d2", ["Storm", "Tempo"]) == {"league_id": "d2", "created": False}
    assert resolve(db, "Ball Park Summer League D2 Finals FIBA", "d2", ["Storm", "Tempo"]) == {"league_id": "d2f", "created": False}
    assert len(db.tables["competitions"]) == 2


def test_unlisted_title_with_nothing_picked_falls_back_to_name_lookup(monkeypatch):
    calls = []
    monkeypatch.setattr(pdf_parser, "get_or_create_league", lambda name, user_id: calls.append(name) or "by-name")
    db = FakeDB([SEASON])
    assert resolve(db, "Some League FIBA", None, ["A", "B"]) == {"league_id": "by-name", "created": False}
    assert calls == ["Some League FIBA"]


def test_unknown_competition_id_is_an_error():
    assert "error" in resolve(FakeDB(), "Anything", "missing", ["A", "B"])
