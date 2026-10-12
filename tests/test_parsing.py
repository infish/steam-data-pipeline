import json
import unittest
from datetime import date
from pathlib import Path

from parsing.names import split_company_names
from parsing.store import (
    parse_languages,
    parse_release_date,
    parse_steamspy_tags,
    parse_store_details,
)


FIXTURES = Path(__file__).parent / "fixtures" / "sources"


def fixture(path):
    return json.loads((FIXTURES / path).read_text())


class SplitCompanyNamesTests(unittest.TestCase):
    def test_plain_list(self):
        self.assertEqual(
            split_company_names("Electronic Arts, Respawn Entertainment"),
            ["Electronic Arts", "Respawn Entertainment"]
        )

    def test_commas_inside_company_names_are_kept(self):
        cases = {
            "Valve, Inc.": ["Valve, Inc."],
            "Bandai Namco Entertainment Co., Ltd.":
                ["Bandai Namco Entertainment Co., Ltd."],
            "Paradox Interactive, Paradox Development Studio, AB":
                ["Paradox Interactive", "Paradox Development Studio, AB"],
            "Ubisoft, Ubisoft Montreal, S.A.":
                ["Ubisoft", "Ubisoft Montreal, S.A."],
            "Feral Interactive (Mac), Feral Interactive (Linux)":
                ["Feral Interactive (Mac)", "Feral Interactive (Linux)"],
        }

        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(split_company_names(raw), expected)

    def test_empty_and_missing_values(self):
        for raw in (None, "", "   ", ", ,"):
            with self.subTest(raw=raw):
                self.assertEqual(split_company_names(raw), [])

    def test_html_entities_are_decoded(self):
        self.assertEqual(
            split_company_names("Tom &amp; Jerry Games, Caf&eacute; Studio"),
            ["Tom & Jerry Games", "Café Studio"]
        )

    def test_trims_and_deduplicates_case_insensitively(self):
        self.assertEqual(
            split_company_names("  Valve ,valve,  Hidden   Path  "),
            ["Valve", "Hidden Path"]
        )

    def test_a_leading_suffix_is_its_own_name(self):
        # Nothing to attach it to; keep it rather than drop data.
        self.assertEqual(split_company_names("Inc., Valve"), ["Inc.", "Valve"])


class ReleaseDateTests(unittest.TestCase):
    def test_complete_dates_in_store_formats(self):
        cases = {
            "Aug 21, 2012": date(2012, 8, 21),
            "21 Aug, 2012": date(2012, 8, 21),
            "August 21, 2012": date(2012, 8, 21),
            " Apr  18, 2011 ": date(2011, 4, 18),
        }

        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(parse_release_date(text), expected)

    def test_partial_or_free_text_dates_are_none(self):
        for text in ("Coming soon", "Q1 2027", "Aug 2012", "2012",
                     "To be announced", "", None):
            with self.subTest(text=text):
                self.assertIsNone(parse_release_date(text))


class LanguageTests(unittest.TestCase):
    def test_full_audio_marker_and_footer(self):
        raw = (
            "English<strong>*</strong>, French<strong>*</strong>, Czech, "
            "Spanish - Spain<br><strong>*</strong>languages with full audio "
            "support"
        )

        self.assertEqual(
            parse_languages(raw),
            [
                ("English", True),
                ("French", True),
                ("Czech", False),
                ("Spanish - Spain", False),
            ]
        )

    def test_recorded_store_response(self):
        data = fixture("store_appdetails/620.json")["620"]["data"]
        languages = dict(parse_languages(data["supported_languages"]))

        self.assertTrue(languages["English"])
        self.assertFalse(languages["Czech"])
        self.assertNotIn("languages with full audio support", languages)

    def test_missing(self):
        self.assertEqual(parse_languages(None), [])


class StoreDetailsTests(unittest.TestCase):
    def test_full_recorded_response(self):
        details = parse_store_details(
            620,
            fixture("store_appdetails/620.json")
        )

        self.assertTrue(details["available"])
        self.assertEqual(details["app_type"], "game")
        self.assertEqual(details["release_date"], date(2011, 4, 18))
        self.assertEqual(details["release_date_text"], "Apr 18, 2011")
        self.assertFalse(details["coming_soon"])
        self.assertFalse(details["is_free"])
        self.assertEqual(details["metacritic_score"], 95)
        self.assertEqual(
            (details["platform_windows"], details["platform_mac"],
             details["platform_linux"]),
            (True, False, True)
        )
        self.assertEqual(details["recommendations_total"], 390960)
        self.assertIn((1, "Action"), details["genres"])
        self.assertEqual(len(details["categories"]), 32)
        self.assertEqual(details["price_overview"]["currency"], "USD")

    def test_missing_keys_are_unknown_not_zero(self):
        # Counter-Strike 2: free to play, no Metacritic score.
        details = parse_store_details(
            730,
            fixture("store_appdetails/730.json")
        )

        self.assertTrue(details["is_free"])
        self.assertIsNone(details["metacritic_score"])
        self.assertIsNone(details["price_overview"])

    def test_unsuccessful_response(self):
        self.assertEqual(
            parse_store_details(1, {"1": {"success": False}}),
            {"appid": 1, "available": False}
        )
        self.assertEqual(
            parse_store_details(1, None),
            {"appid": 1, "available": False}
        )


class SteamSpyTagTests(unittest.TestCase):
    def test_recorded_response(self):
        tags = dict(parse_steamspy_tags(
            fixture("steamspy_appdetails/427520.json")
        ))

        self.assertEqual(len(tags), 20)
        self.assertEqual(tags["Automation"], 4756)

    def test_apps_without_tags(self):
        self.assertEqual(parse_steamspy_tags({"tags": []}), [])
        self.assertEqual(parse_steamspy_tags({}), [])


if __name__ == "__main__":
    unittest.main()
