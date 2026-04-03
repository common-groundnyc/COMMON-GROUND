import pytest
from dagster_pipeline.defs.name_tokens_asset import tokenize_name, STOPWORDS


class TestTokenizeName:
    def test_simple_name(self):
        assert tokenize_name("JOHN SMITH") == ["JOHN", "SMITH"]

    def test_strips_stopwords(self):
        tokens = tokenize_name("THE BLACKSTONE GROUP LLC")
        assert "THE" not in tokens
        assert "LLC" not in tokens
        assert "BLACKSTONE" in tokens
        assert "GROUP" in tokens

    def test_short_tokens_excluded(self):
        tokens = tokenize_name("A B SMITH")
        assert "A" not in tokens
        assert "B" not in tokens
        assert "SMITH" in tokens

    def test_empty_string(self):
        assert tokenize_name("") == []

    def test_none_returns_empty(self):
        assert tokenize_name(None) == []

    def test_whitespace_handling(self):
        assert tokenize_name("  JOHN   SMITH  ") == ["JOHN", "SMITH"]

    def test_uppercase(self):
        assert tokenize_name("john smith") == ["JOHN", "SMITH"]

    def test_hyphenated_name(self):
        tokens = tokenize_name("MARY SMITH-JONES")
        assert "SMITH-JONES" in tokens

    def test_stopwords_constant(self):
        for word in ["THE", "OF", "AND", "INC", "LLC", "CORP", "LTD", "CO", "NY", "NEW", "YORK"]:
            assert word in STOPWORDS
