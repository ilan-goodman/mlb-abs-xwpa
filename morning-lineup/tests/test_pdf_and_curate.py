from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from morning_lineup.curate import select_articles
from morning_lineup.models import Article
from morning_lineup.pdf import make_pdf


class MorningLineupTests(unittest.TestCase):
    def test_pdf_starts_with_pdf_header(self) -> None:
        article = Article(
            title="Giants win a tidy one",
            url="https://example.com/story",
            source_name="Example Sports",
            published=None,
            text="This is a readable baseball story. " * 80,
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = make_pdf(article, Path(tmp) / "story.pdf")
            self.assertTrue(path.read_bytes().startswith(b"%PDF-1.4"))

    def test_select_articles_skips_similar_titles(self) -> None:
        articles = [
            Article(
                title="Giants rally late to beat Dodgers",
                url="https://a.example/1",
                source_name="A",
                published=None,
                text="x",
                score=20,
            ),
            Article(
                title="Giants rally late and beat the Dodgers",
                url="https://b.example/2",
                source_name="B",
                published=None,
                text="x",
                score=19,
            ),
            Article(
                title="Sacramento River Cats prospect throws six scoreless",
                url="https://c.example/3",
                source_name="C",
                published=None,
                text="x",
                score=18,
            ),
        ]
        selected = select_articles(articles, max_articles=6)
        self.assertEqual([article.title for article in selected], [
            "Giants rally late to beat Dodgers",
            "Sacramento River Cats prospect throws six scoreless",
        ])


if __name__ == "__main__":
    unittest.main()
